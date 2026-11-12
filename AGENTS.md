<!-- rex:codebase -->
## Codebase map

Run one of these commands before you search the tree by hand. Each one prints a tree of the working directory and skips the files that git ignores.

- `rex codebase`: every file.
- `rex codebase --only code`: only source files (Rust, Go, C, C++, Python, JavaScript, TypeScript, shell, Java, Kotlin, Scala, C#, Swift, Objective-C, Dart, PHP, Ruby, Lua, Haskell, Elixir, R, SQL, Zig, Perl).
- `rex codebase --only go`: only one language.
- `rex codebase --with-context`: adds the first sentence of each source file's opening comment to its line.
- `rex codebase --only code --with-context`: the source files and what each one is for. Start here.

Add `--for-human` to write the tree to `CODEBASE.md` instead of printing it, for example `rex codebase --for-human --only code --with-context`.

Every source file opens with a comment of one or two lines that says what the file is for. A file with no sentence after its name in the tree is missing that comment. When you create or change a file, write the comment or bring it up to date.

## Code smells

Run this command to find each known problem that was flagged and left in the code. It reads every `.rs` file and skips the files that git ignores.

- `rex smells`: one `path:line` for each `SMELL` flag comment in the Rust files, then the count of them all.

<!-- instructions -->

Read @CONSTITUTION.md

`.mcp.json.off` is off on purpose: its server has tools that set an engine to `live` and start it with no guard. Do not rename it to `.mcp.json` and do not start its server. The file gets its name back when that guard exists.

Working on src/adapters/? Read src/adapters/exec/README.md first — it is the adapter execution contract.
