## Team Member

Hieu Le (76067321)
Torrin Pataki (77268944)

## How we split task

We worked on the assignment individually at first, each developing our own solution. Once both solutions were completed, we sat down together to discuss our approaches, compare results, and combine ideas. From that discussion, we agreed on the best solution and finalized it collaboratively

## AI Citation

1. We asked Cursor AI to help us generate the workerID -> Prompt: "Please help me createa a function that generate unique ID for worker, possibly uuidv4()"

2. We asked Cursor AI to help us with understand temporary files in GO, and use the code from mrsequential.go to create intermidate files -> Prompt: "Please look at mrsequential.go, explain to me how to create intermediate files in GO during MapReduce"

3. We explained how we want our heatbeat mechanism works, guide Cursor AI through my instructions, and apply Cursor's code -> Prompt: "Please add the heartbeat mechanism into my current implementation, worker will signal coordinator every second to make sure it's still alive, if they worker isn't being responsive, re-assign mark the task as idle, and may re-assign that task to other free worker. Please don't make any changes until you're over 90% confident, ask me clarify questions if needed"

## Instruction:

In the /main directory, run:

```
rm mr-out*
go build -buildmode=plugin mrapps/wc.go
```

and then you can just run the test in /main as well:

```
bash test-mr.sh
```
