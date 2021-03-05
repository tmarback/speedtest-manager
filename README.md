# speedtest-manager

A utility that uses the [Speedtest CLI](https://www.speedtest.net/apps/cli) to repeatedly run Internet connectivity measurements. Naturally, using it requires having the CLI installed.

## Manager

The manager is a long-running process that manages and executes measurements.

## Client

The client connects to the manager using the network to perform management operations and queries. It should be used to add, stop, and remove jobs, as well as retrieve the results of any existing job.
