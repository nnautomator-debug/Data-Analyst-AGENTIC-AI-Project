### Kafka Event streaming for Agentic EVALS 

1. The prooducer of the ticket is the event of completion of user chat . so basically in the mian pyhton file we take in a query and aswer that using our ai data analyst agent . so once the query is done answering and the langgraph state vector finishes its execution we will start a series of events that will follow by 
    - the events that will follow by will subscribe to the main producer topic. 
    - the producer is when the agentic system completes its execution . 

2. Series of events that will follow : 
 
    a. Logging consumer . so this will be the first subscriber to the producer event . after the agentic run is finished , the logging system logs the following about the run into the database which in this 
                          we will be using duckdb . 
        - the following are the necessary columns that are presnt in the duckdb database we build. so each row represents one run(end to end langgraph state vector run for a given query): 
            1. sl no 
            2. user query 
            3. time stamp of agent start 
            4. dataset profile 
            5. plan agent's returned plan . 
            6. execution agent's code  with output(2 seperate columns)
            7. code check agent(its output whether it passed or not , if failed then the failed code will be added here with the reasoning we just built)- so here if passed , then we say passed , if failed we add the wrong code + reasoning here. 
            8. synthesis - this will be the final ai produced answer. 
            9. time stamp of agent run end 
        - a database write query has to be done which writes all this to the duckdb (main logging mechanism)
    
    b. EVALS - this is the main heart of our agentic ai system. so this is where we will collect and create a golden dataset to imrpve an agent iteratively with time . so eval golden dataset collecition is the primary job of this consumer. 
        - so this consumer will work to sort the incoming examples into a golden dataset that is used to re train, re evaluate and improve the agent. 
        - what is necessry is a mechanism to sort a given run data as golden for eval or not 
        - for the above we will be using 4 main metrics or components we can say : 
            1. Re gex patterns -> this one is quite simple , after the whole run we get the code outputs of each sandbox run that happened(exec agent + code check agent) and we use regex patterns to identify the error and spot them . Since all runs will not have errors , only the errored runs will be filtered out by this step. 

            2. Human feedback - this will be in the UI , so i dont want you to focus on the value of this for now , just create it and leave it until we build the ui. so basically we will have a thumbs up or down button for the user , if thumbs down we will ask an optional descriptive user feedback so that w can improve and this user feedback collection will be crucial for evals. 

            3. LLM as a judge eval - so this one runs on the following things: 
                a. it has access to the user query , code scripts total runs - including retried ones by code check agent as well as the exec agent runs. 
                b. it also has access to the plan of the plan agent and the final synthesis . 
                c. it looks at all this and identifies bottlenecks with citations within 100 words so that we can use this to improve. 
                d. model - use groq with opnai gpt oss 120b. 

## Details 
- what i have described is the architecture of the whole kafka event BUS that we will run after the agent completes. 
- so i need you to build this with a pre plan phase before writing code. 
- kafka docs - "https://kafka-python.readthedocs.io/en/master/apidoc/modules.html" - webfetch and keep the code clean
- use kafka with uv add and kafka for pyhton make sure to use. 
- we have a producer and consumer . we will chose the consumers accordingly as i have described and producer will be completion of agentic run . 




