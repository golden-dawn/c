### Debugging a process with lldb


Compile the executable with `debug_compile.sh`:  
```
./debug_compile.sh stx_ana.c
```

Launch lldb:  

```
lldb stx_ana.c
(lldb) target create "stx_ana.c"
(lldb) b stx_ht.h:138
Breakpoint 1: no locations (pending).
Breakpoint set in dummy target, will get copied into future targets.
(lldb) file stx_ana\\.exe
Current executable set to 'stx_ana\.exe' (x86_64).
(lldb) b stx_ht.h:138
Breakpoint 2: where = stx_ana\.exe`ht_insert + 165 at stx_ht.h:139:24, address = 0x0000000100000e25
(lldb) process launch
(lldb) p crt_item
(lldb) p index
(lldb) p (crt_item != NULL)
```