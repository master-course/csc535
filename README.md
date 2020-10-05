

## `Introduction`

In this lab assignments I write a basic database management system called SimpleDB. I focus on implementing the core modules required to access stored data on disk.SimpleDB is written in Java, which mean the user need to be familliar with Java programming language and building flow. For building the environment, I use the `ant` builder. And the code of the builder can be found in `build.xml`.


## `Running the SimpleDB`

   - `Installation`
    
        To be able to run the simpleDB, clone the repository as showing below:

        ```sh
        $ git clone https://github.com/master-course/csc535.git
        ```

   - `Running the test`

        To check the implementation of the simpleDB, navigate to the root of your cloned repository and run the following test.

        - `UnitTest`
  
            ```sh
            $ ant test
            ```

        - `SystemTest`

            ```sh
            $ ant systemtest
            ```