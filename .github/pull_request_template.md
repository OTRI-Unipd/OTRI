## Premise
__Address here what was the problem, the trello board, or the issue you are taking care of.__

## Feature
__List of changes, use semantic versioning's (or conventional commit's) rules. Be specific about important things that need to be understood, ignore self-explaining changes.__
List files with keyword as lvl 4 titles and then list internal file changes
Possible keywords for files:

**feat(filename)** when adding new features
**fix(filename)** when fixing something not properly working
**syle(filename)** when organizing and styling the script
**doc(filename)** when adding documentation
**refactor(filename)** when refactoring
**test(filename)** when adding unit tests

When listing:

**+** Use plus for additions
**-** Use minus for deletions/code removals
**~** Use tilde for changes

Remember to surround them with double * otherwise they'll be changed to list bullets

Example:

#### feat(`downloader.py`)
**+** Added `choose_folder()` method that lets you decide where to save data
**~** Changed how files are ordered when listed
**-** Removed `do_nothing()` mehtod