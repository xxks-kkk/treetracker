15W.json, 18W.json, and 20W.json are manully generated Postgres plans based on 
15W.json.ignore, 18W.json.ignore, and 20W.json.ignore, which are originally Postgres plans.

We do this to simplify the code of generating query implementation based on plans.
These two plans are complicated to process; so we manually generate json plans so that 
we can proceed without changing much code for query generation.
