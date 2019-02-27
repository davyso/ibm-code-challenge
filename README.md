# ibm-code-challenge (Spark)

## Setup

#### IntelliJ Scala Plugin
Since I used IntelliJ for my development, I will explain how I set up the development environment. First, the Scala 
plugin is needed. It can be found here: `File` -> `Settings...` -> `Plugins` . Search for Scala in the search bar.

#### Scala Setup
Thanks to Maven, the Scala language should be downloaded for within the scope of this project. While IntelliJ has the 
Scala plugin, it needs to identify the compiler for it. On the top of the editor should be a yellow bar notifying 
"No Scala SDK in module". On the far right, there should be a clickable link called "Setup Scala SDK". From here, do the
following:
- It will say "\[No library selected\]". Click on "Create..."
- There may be 1 or 2 options. Highlight the one that says Maven and Scala 2.11.12 (if multiple, pick any).
- It should now say "scala-sdk-2.11.12". Click OK


#### Libraries/Dependencies
For Windows machines, the global Hadoop library is needed, which it appears to already exist in MacOS. Fortunately, that 
Hadoop library is bootstrapped within this project. You will just need to set the following environment variable in 
IntelliJ (`Run` -> `Edit Configurations...`):
```
HADOOP_HOME=$PROJECT_DIR$\hadoop-2.7.1\
```
The other dependencies of the tech stack (Scala, Spark, etc.) should be setup using IntelliJ's provided Maven.

## Running Jumbles
The class `DeJumblerTest` contains all 5 Jumble puzzle examples. You can run all of them at once or one at a time. The 
test suite also contains unit tests that you may choose to run.

## Solving Cartoon
Sadly, I had trouble solving the cartoon at the end of each puzzle. This is because a large variety of possible answers 
that would require each node to talk to other nodes (at least, this is what I figured out so far). For example, provided
with circle letters `List('l', 'n', 'd', 'j', 'o', 'b', 'e', 'a', 'l', 'w', 'e')`, and the sizes of the circle words are
3, 4, and 4, then all possible ways to fit the circle letters into the first 3-letter x second 4-letter word x third 
4-lettter word would need to be found which would be extremely high. While somewhat runnable but not complete, severel 
attempts (methods `solveCircles()` and `solveCircles2()`) have been made.