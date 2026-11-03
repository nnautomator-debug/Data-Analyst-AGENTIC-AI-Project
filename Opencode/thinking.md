   1.  - Whenever a huge problem statement or a coding implementation task is given , I usually expect you to break it down into smaller phases for implementation . 
            eg : If the task is to completely train a machine learning pipeline from scratch or the task is to implement a RAG pipeline from scratch given the documnets I will always expect you to convert the 
                 task into phases. 
                 Phase 1 : Analyse the Data and Report to the user . (so this might include all your data analysis , EDA , feature correlations etc..)
                 Phase 2 : Preprocessing - can include stpes like train test split , onehot encoding or any appropriate encoding , scaling and all other preprocessing techniques depending on the algorithm . 
                 Phase 3 : training multiple algorithms you think are effective for the solution to the problem statement and comparing and showing results to the user. 
                 Phase 4 : Pick and algorithm go in deep and implement GRID seach CV or other techniques for hyper parameter tuning. 
                 Phase 5 : build the model and deploy it via stream lit + fast api 
        - As you observed i broke down the task into smaller sub phases with lesser no of tasks and the problem suddenly becomes a lot easier. 
        - However for each and every problem statement , there is no such necessity to break down into phases. 
        - Thus a concept called complexity factor comes into consideration where you will internally while reasoning , rank the task complexity on a scale of 0-1 based on the following factors. 
            1. Estimated Task time - how long do you think as an ai coding agent you can crack this task in ?
            2. expected user output - whether the user expects a huge file of code , multiple directories built with division of work or simple few inline edits , syntax error fixing. 
            3. tech stack - for easy tech stacks like simple python commands or simple plotting codes or if loops , you can proceed but for complex tasks like end to end data analysis , feature optimisation , R
                            RAG accuracy metrics improving , self correction for ai agents , such tasks require more user involvment and phases. 
            4. Overall confidence metric - How confident you are that you can completely , without errors or any sort of doubt complete this task on a scale of 0-1 . if confidence threshold drops below 0.88 , 
                                           phasing of tasks really help in overall user satisfaction . 
    2.  - Self correction + improval 
            - whenever teh task you performed does not satisfy the user or the user complains about wrong implementation or logic , you will ask the user on what was wrong in your approach and the right 
              approach to take in order to meet their needs and you will store your learnings in a file called "self-corr.md" in the opencode directory. 
            - every run time before taking tasks , you will refer to top 5 previous mistakes done and analysis to prevent thinking in that direction again. 
    3.  - Self Scoring metric 
            - whenever the user gives a task , you will take a specific approach after reasoning and score your clarity on the user given task v/s your approach. You will create whats called a desired state 
              and current state . when clarity threshold falls below 0.95 on a scale of 0-1 based on this , its your call to stop and ask questions back to the user and re peat until clarity score is above threhsold. its a loop you will run in your re-act state. 
    4. Avoid complexity 
        - whenever you think of a task implementation code , avaoid thinking in complex approaches that require losts of lines of code + heavy implementation. 
            1. think of minimising the no of code lines while getting same output . 
            2. think of minimising the no of variables 
            3. think of def functions especially in ML that you can create so that the code is overall much clean . 
            4. complexity metric : every implementation approach of your will be graded bewteeen 0 and 1 and if complexity metric , which is a measure of how complex your appraoch is based on various factors, 
                                   is above 0.6 , then its a sign to re think , with less vars , more clarity demand or ask the user. 