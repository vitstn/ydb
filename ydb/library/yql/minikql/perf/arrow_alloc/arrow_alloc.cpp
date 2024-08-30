#include <ydb/library/yql/minikql/mkql_alloc.h>

#include <util/stream/file.h>
#include <util/string/split.h>
#include <util/string/strip.h>

enum class EMethod {
    Allocate,
    Free
};

struct TCommand {
    ui64 Size;
    EMethod Method;
};

int main(int argc, char* argv[]) {
    if (argc < 2) {
        Cerr << "Expected trace file(s) name\n";
        return 1;
    }

    TVector<TCommand> commands;
    for (int i = 1; i < argc; ++i) {
        TFileInput input(argv[i]);
        for (;;) {
            TString s;
            if (!input.ReadLine(s)) {
                break;
            }
            
            TCommand command;
            ui32 part = 0;
            for (TStringBuf p: StringSplitter(s).SplitByString(",")) {
                auto s = Strip(TString(p));
                if (part == 2) {
                    if (s == "Allocate") {
                        command.Method = EMethod::Allocate;
                    } else if (s == "Free") {
                        command.Method = EMethod::Free;
                    } else {
                        Cerr << "Wrong input file format\n";
                        return 1;
                    }
                } else if (part == 3) {
                    command.Size = FromString<ui64>(s);
                }

                ++part;
            }

            if (part != 4) {
                Cerr << "Wrong input file format\n";
                return 1;
            }
            
            commands.push_back(command);
        }
    }

    Cerr << "Loaded " << commands.size() << " commands\n";
    i64 currMem = 0;
    ui64 totalAlloc = 0;
    i64 maxCurrMem = 0;
    for (const auto& c : commands) {
        if (c.Method == EMethod::Allocate) {
            currMem += c.Size;
            totalAlloc += c.Size;
            maxCurrMem = Max(maxCurrMem, currMem);
        } else {
            currMem -= c.Size;
            if (currMem < 0) {
                Cerr << "Found double free\n";
                return 1;
            }
        }
    }

    Cerr << "Max memory usage: " << maxCurrMem << "\n";
    Cerr << "Final memory usage: " << currMem << "\n";
    Cerr << "Total allocations: " << totalAlloc << "\n";
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    THashMap<ui64, TVector<void*>> blocks;
    THashMap<ui64, ui64> peakBlockCount;
    for (const auto& c : commands) {
        if (c.Method == EMethod::Allocate) {
            auto ptr = UdfArrowAllocate(c.Size);
            blocks[c.Size].push_back(ptr);
            memset(ptr, 0, c.Size);
            peakBlockCount[c.Size] = Max(peakBlockCount[c.Size], blocks[c.Size].size());
        } else {
            auto& v = blocks[c.Size];
            if (v.empty()) {
                Cerr << "Found free without alloc for size: " << c.Size << "\n";
                return 1;
            }

            UdfArrowFree(v.back(), c.Size);
            v.pop_back();
        }
    }

    NKikimr::PrintGlobalPoolArrowStats();
    Cerr << "Unique block sizes: " << "\n";
    TVector<ui64> blockSizes;
    for (auto& [size,v] : blocks) {
        blockSizes.push_back(size);
    }

    Sort(blockSizes);
    for (auto size : blockSizes) {
        auto realSize = AlignUp<ui64>(size, 4096);
        if (realSize < 65536) {
            realSize = 65536;
        }

        if (realSize <= 64 * 1024 * 1024) {
            realSize = FastClp2(realSize);
        }

        Cerr << size << ", peak count: " << peakBlockCount[size] << ", peak size: " << size * peakBlockCount[size] 
            << ", real size: " << realSize * peakBlockCount[size] << "\n";
    }

    for (auto& [size,v] : blocks) {
        for (auto ptr : v) {
            UdfArrowFree(ptr, size);
        }
    }

    Cerr << "Done\n";
    NKikimr::PrintGlobalPoolArrowStats();
    return 0;
}

