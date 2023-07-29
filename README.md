# Customer.io Challenge README

### Project Description

For this project, you will write a Go application that summarizes two types of **user behavioural data** stored in a JSON-encoded file. This file contains one activity item per line, each tied to a **user_id**, of the following types:

- **One-time events**, which represent activities performed by a user at specific point in time
- **Attribute changes,** which represent the setting of **persistent attribute values** for the user at a specific point in time

**Example Events:**

```
{"id":"c7d1a8d9-da03-11e4-87ec-946849a0cf6a","type":"event","name":"page","user_id":"2352","data":{“url”: “http://mystore.com/product/socks”},"timestamp":1428067050}
{"id":"735a247d-7179-5024-1686-ab353a730b45","type":"event","name":"purchase","user_id":"2352","data":{“sku”: “CMR01”, “price”: 19.99},"timestamp":1428067050}
```

**Example attribute change:**

```
{"id":"c52543d8-da03-11e4-8e29-c5dc2fe5941b","type":"attributes","user_id":"2352","data":{“name”: “Bill”, “email”: “bill@gmail.com”},"timestamp":1428067050}
```

Using the code in this **.tar** file as a base, your program should iterate through each line of an input file and for each unique **user_id** present in the file:

- Keep a record of the **latest value** for each set attribute, where latest means the **most recent timestamp specified in an attribute message for a user_id/attribute pair.** The attributes being set are stored in the **data** hash of the attribute change message
- Keep a count of the **unique number of times a given event type was performed** for this user_id, taking into account the possibility of duplicated IDs

After processing the entire file, the program should write a summary for each unique **user_id** to a **newline-delimited** output file, in ascending order by **user_id**. The format of each line in this file should be:

```
user_id,attr=value,attr=value,event=count,event=count
```

Where attribute and event names are **sorted in ascending order from left to right**

**Example summary line for the above data:**

```
2352,email=bill@gmail.com,name=Bill,page=1,purchase=1
```

There won’t be any commas, equals signs or newline characters in the user ids or attribute/event names/values, so you don't need to worry about escaping your output

### Setting up your environment

You should be using **go 1.15 or later**

We recommend developing using Visual Studio Code https://code.visualstudio.com/ and the vscode-go plugin https://github.com/Microsoft/vscode-go or a similar configuration. This will set up some of the standard tooling you need to get started with a go project. Feel free to use external packages, which you can install with `go get packagename`.

Some useful resources for getting started:
- Effective Go: https://golang.org/doc/effective_go.html 
- Go Styleguide: https://github.com/golang/go/wiki/CodeReviewComments

But don't worry about having perfectly styled Go, especially if you're new to the language. We're more interested in the functionality of your solution than the specifics of style.

This **.tar** file contains the following:

- A basic version of a main program `main.go` which reads and parses the input file line by line, providing you with a channel that you can pull records from. You can choose to use this, but it's fine if you'd prefer to write your own. To run the main program you can use `go run main.go`
- A program which you can use to generate test data. The `generate/main.go` program generates two files: a .**data** file, which contains the input JSON data and a **.csv** file which contains a reference summary file.
- The basic program contains a function you can use to check the output of your program against the reference summary file.

 We recommend generating three test datasets in the **data/** directory as follows:

```
go run generate/main.go -out data/messages.1.data -verify data/verify.1.csv --seed 1560981440 -count 20
go run generate/main.go -out data/messages.2.data -verify data/verify.2.csv --seed 1560980000 -count 10000 -attrs 20 -events 300000 -maxevents 500 -dupes 10
go run generate/main.go -out data/messages.3.data -verify data/verify.3.csv --seed 1560000000 -count 1000000 -attrs 10 -events 5000000 -maxevents 10 -dupes 20
```

### Two Solutions

1. The first solution should solve the basic constraints and be as simple as possible. Correctness and readability are the key requirements here. This solution can run all in memory, and isn't subject to any RAM constraints, and does not need to support restart or be fast.

    source code for this solution is located at `/solutiona/main.go`
    

2. A second solution should extend the first solution, and support large datasets without keeping all data resident in memory.
    - There are resource constraints and your solution must not exceed them for any given dataset up to the maximums detailed below. **Your solution must not use more than 1GB of RAM and 10GB of Storage.** We will test this by running your solution on a 64-bit Ubuntu Machine with 1GB of RAM and a 100GB SSD. If your solution requires any installed applications (databases, caches, etc.) we will install with the OS defaults and we will not apply any tuning to the configuration.
    - We will be testing your solution with a file containing **at least 1 million** unique user_id values, **at least 10 million** unique events and **~10 attributes** per user
    - Your solution should be fault tolerant: expect your program (and/or any database you use) to be force killed and restarted while processing the file. This shouldn’t lead to any incorrect counts, etc.
    - Bonus points if you can maintain your state and position in the file, to prevent the need to reprocess from the beginning after restart

### Things we'll want to talk about

- Why did you choose the architecture used in your solution?
- What other architectures could you envision for this problem?
- What assumptions did you make?
- Where are the bottlenecks? What's using the most memory, the most cpu, the most time?
- How would you improve the performance?
- After these are covered we'll want to discuss how to extend your solution in various ways.
