# Workflow
We need to have an execution framework that lets us execute data preparation tasks. It is recognized that we will be using Kubernetes to manage long-running server tasks, but what do we use to generate the data used by those tasks? How do we manage downloading data and such?

The requirements that such a framework needs to meet include:

1. It can execute a workflow consisting of chained tasks. Each task processes some inputs and produces some outputs.
1. These inputs and outputs consist of directories of data files.
1. The chained tasks need to run in containers on Kubernetes.
1. It should be easy to deploy a test version of part or all of a workflow so that we can measure how well it works. Such a test version shouldn't stomp on production data.
1. We need to have logs and historical execution parameters like runtime recorded for post mortem analysis.
1. It should be easy to backfill workflow results if an error is detected. It should be possible to backfill for specific time ranges and it would be nice if we could control whether backfilling replaces most recent results first or oldest results first.
1. It should be possible to describe an entire directory full of files as a single data entity, but to detect when a subset of the files in the directory have changed and only trigger downstream tasks for the subset of files that need processing.
1. It is desirable that the workflow engine itself launches tasks. Further, it is desirable if the tasks are launched with a particular identity specific to the task and the user who installs the workflow or workflow fragment. This will let us use file permissions and user identities to control access to sensitive data (if we have any) and to protect production outputs from test runs.

## Code-centric workflows
One approach to building a workflow such as we are talking about here is to define code for each task and connect the tasks together explicitly. Version control on the code and workflow definition allows us to deploy new versions of tasks or workflows.

This code-centric approach is typically weak on data versioning. The assumption usually is that if you deploy a new version of a workflow, it should entirely replace the old version and use the same data locations. If you are running a test version, it is assumed that you splice this test version into the global workflow and write to new output locations. A few systems (like flyte) have some ability to memoize task execution so that repeatedly running the same task on the same inputs is avoided, but most code-centric systems don't seem to automagically detect when a new version of a task produces identical output. If they did so, all downstream outputs created by unchanged tasks could conceivably be virtualized, but making such virtual outputs appear to be independent files without executing the associated tasks is generally outside the scope of a code-centric workflow system.

Systems in the code-centric category include Airflow, Luigi, Dagster and Flyte

## Data-centric processing
An alternative organizing principle centers around some concept of data version control. Since data files can be very large, it is impractical to use a system like `git` for this. Instead, these systems provide alternative version control methods that use git-like concepts to present virtual data elements that are typically a reference to a file cache or references to a proxy that supports the S3 API, but which assembles components of files stored on an external object store rather than storing the data directly.

Data-centric systems usually require that you insert work steps that register outputs with the framework. Some systems like DVC and Pachyderm also record the data processing steps that link one data registration and the next. Such systems can replay the execution of these processing steps in a manner similar to the way that a code-centric system executes tasks. Where these systems fundamentally diverge, however, is that the data-centric system can know precisely if an output file duplicates the data from some other version of the data because it can examine the checksums of alternative versions of a file. If a duplication is found, the code-centric framework can replace the output file with hard or soft links to a cached version of the file. A data-centric system is thus in a much better position to avoid redundant work due to trivial changes to a processing step.

Another difference between data-centric and code-centric systems is that in a data-centric system, code versions are entailed in the data version control. This can make it hard to trigger a data-centric system automatically based on a code change. A code-centric system, on the other hand entails changes to data as an implicit side effect of well controlled versions of code. That makes it easier to talk about code versions, but harder to track redundancy on the data side.

An issue that comes up with data-centric systems is that if you manage large data elements using such a system, that data may be stored in a relatively opaque internal form that is difficult to access directly without the framework. This causes an organic form of lock-in which can make it very difficult to migrate to an alternative framework because extracting data is expensive. In some cases, extracting you data requires back filling the process that created the data under management. Such a migration typically will retain the lineage information for your data, but will lose data versioning information.

Systems in the data-centric category include DVC and Pachyderm.

# System comparisons
## Airflow
https://airflow.apache.org/
https://twitter.com/holdenkarau/status/1429206463294021634?s=20

Airflow is a widely used system for managing code-centric workflows. Workflows are created by annotating a Python program that defines a DAG for a particular purpose. This definition specifies a recurrence schedule for invoking the DAG as well as the dependencies of various processing steps. Each processing step can invoke many other kinds of processing other than Python if necessary, but each processing step is typically deployed as an individual container to maximize commonality between the development and production environments.

Airflow and Luigi are very similar in many respects and it is possible to build processing steps that can be executed by either framework. Some important differences from Luigi include 
1. the ability of Airflow to update workflow invocations rather than depending on an outside system to trigger work the way that Luigi does and 
1. Airflow is much more widely used and supported than Luigi
1. Luigi is more command line oriented and Airflow has more emphasis on a web interface

In common with Luigi, Airflow has no concept of data version control. Presumably a system like DVC could be used to record the actions taken by Airflow to form a data lineage.

See https://databand.ai/blog/everyday-data-engineering-tips-on-developing-production-pipelines-in-airflow/ for commentary on how to develop for Airflow.
## Luigi
https://luigi.readthedocs.io/en/stable/
https://twitter.com/lalleal/status/1429196078193250312?s=20

Luigi is a very low-level system for managing code-centric workflows. Incremental data flows are customarily handled by programmatically augmenting the processing DAG for new input files. There is no concept of data version control in Luigi and many steps in processing maintenance involve manual effort and a knowledge of the internals.

Workflows progress through scheduled invocation of programs that query the state of the workflow and then do the corresponding work. Luigi itself does not actually manage or invoke the workflows at all. This design allows very large workflows to be managed, but it increases the operational complexity.

See https://www.youtube.com/watch?v=fYJspPFo2jU for a taste of the philosophy.
## DVC
https://dvc.org/

DVC grew out of common experiences on the part of a number of data scientists as they recognized that they were repeatedly working around the problem of building reproducible data pipelines by putting parameter settings and references to (presumably) immutable data objects into ordinary files which were then checked into git. That practice allows precise replay of machine learning processes and with a small amount of scripting allows leaderboards to be created showing the performance achieved by alternative model building processes.

DVC augments this very basic idea with standard methods for recording the processing steps used to transform data into models and then test those models. The result is a set of commands that encapsulate these practices and allow processing pipelines to be run repeatably and precisely. DVC also includes a web-based dashboard so that you can compare figures of merit of different parameter settings or program versions. As a side effect, even though DVC is unabashedly data-centric you wind up with a DAG of processing steps very similar to those managed by Airflow or Luigi. You have to initiate the processing done by these steps and there is very rudimentary support for parallel execution, but the similarity is definitely still there.

Unfortunately for AgStack usage, DVC is heavily focussed around the model building steps in a data pipeline and making iteration on those final steps efficient. It is not designed for full scale data pipelines that involve substantial data transformation before feature extraction and model building. This focus is very apparent in the way that DVC does not have any good way to incrementally process a directory full of data files as new files appear over time. It may be possible to extend DVC to these use cases, but it isn't clear that it will work well. 

DVC has a mechanism called the [`run-cache`](https://dvc.org/doc/user-guide/project-structure/internal-files#run-cache) which is intended to optimize away redundant processing steps. This is important when you have staging and production workflows that contain many of the same steps, often working on the same input files with the same code. When such redundant steps are detected in a data-centric system, it is reasonable to simply checkout out the results that were previously produced in some other DAG. This can even be extended so that if a work step produces a result file that is identical to a previously produced, that fact can be recorded and many work steps avoided.

In practice, this kind of optimization can avoid 90% or more of the work required in production workflows, particularly when there are many workflows that share processing steps or where multiple shadow and staging versions of the production workflow are in operation at the same time.

DVC is also heavily oriented around versioning of files rather than S3-compatible objects. This is done through heavy use of symbolic and hard links to cached versions of files. Versioning of objects is not well supported, but the rough effect can be had if input URLs are injected into processing steps by the processing framework.
## Pachyderm
Pachyderm is a data-oriented system similar in many ways to DVC that starts with the notion of version controlling data assets and recording the processing steps that transform inputs into outputs. Aspects that distinguish Pachyderm from DVC include:
1. Pachyderm is focussed on the problem of data pipelines more than the terminal problem of comparing model building variations
1. Pachyderm is heavily focussed on the elimination of redundant processing steps by detecting identical inputs and identical processing steps.
1. Pachyderm is heavily oriented around Kubernetes as a mechanism for managing computation. Parallel execution of processing steps and strong container orientation are natural logical consequences of this. DVC, on the other hand, has only rudimentary ways to invoke processing steps.
1. Pachyderm aggresively caches data by splitting files and objects into roughly 5-10MB chunks and storing these in an object store. Access to version controlled objects is done via a proxy that reassembles these chunks into the desired content.

https://www.pachyderm.com/
https://twitter.com/tristanzajonc/status/1429208490094960641?s=20
## Dagster
https://dagster.io/
https://twitter.com/schrockn/status/1429209827494875139?s=20
## Flyte
https://docs.flyte.org/en/latest/
https://twitter.com/soebrunk/status/1429539368558006273?s=20
## Metaflow
https://metaflow.org/
## Gitlab's CI
## Project Nessie
https://twitter.com/patrickangeles/status/1429254409796657153?s=20
