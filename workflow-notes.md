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

## Luigi
https://luigi.readthedocs.io/en/stable/
https://twitter.com/lalleal/status/1429196078193250312?s=20
## Dagster
https://dagster.io/
https://twitter.com/schrockn/status/1429209827494875139?s=20
## Flyte
https://docs.flyte.org/en/latest/
https://twitter.com/soebrunk/status/1429539368558006273?s=20
## DVC
https://dvc.org/
## Pachyderm
https://www.pachyderm.com/
https://twitter.com/tristanzajonc/status/1429208490094960641?s=20
## Metaflow
https://metaflow.org/
## Gitlab's CI
## Project Nessie
https://twitter.com/patrickangeles/status/1429254409796657153?s=20
## Azkaban
https://azkaban.github.io/
