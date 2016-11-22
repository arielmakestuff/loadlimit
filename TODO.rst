====
TODO
====

* Develop import hook to take tasks from standalone files 

  - Use loadlimit.task virtual module namespace

* Implement timeout coro func

  - This runs the load test until a specified datetime and then shuts the
    loop down

* Implement cli interface

  - Integrates tdqm for output display

  - Primary progress bar should be time to complete
    
  - Secondary progress bar should output iter/s

* aiohttp integration

  - Create special class with async __call__()

  - Must add a new column to results for size? (Not necessary)

  - Should be an extension not built into the program 

    * Use loadlimit.ext virtual module namespace

* Export results to csv

  - Both distribution and general results
