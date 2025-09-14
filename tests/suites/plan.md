# Rules for code authoring
- Do "Todo List" in order, address the first item first
- After writing any new code and before moving on to another prompt, follow "Instructions for each code authoring iteration"

# Instructions for each code authoring iteration
- Write new code to codebase
- Run tests using tests/run-tests.sh.  Tests CANNOT be ran on the host machine and must run through the vagrant wrapper tests/run-tests.sh
- Check readme

# Todo List
- Review the readme and improve it
- Add more tests for missing areas
- Finish writing tests that have # Skip this test for now as it's complex to mock properly
- Rewrite tests to use the central pipeline handler