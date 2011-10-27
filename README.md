# cassandra-syncer

A tool for syncing Csasandra SSTables out to various destinations.

# Introduction

Example saving to Cloudfiles:

    bin/cassandra-syncer --dest cloudfiles://user:password@cloudfiles/bucket-name --source /var/lib/cassandra/data

Example saving to another local path, which could be a network volume like EBS:

    bin/cassandra-syncer --dest directory:///mnt/network-volume --source /var/lib/cassandra/data

Example restoring from the backup:

    bin/cassandra-syncer-restore --source directory:///mnt/network-volume --dest /var/lib/cassandra/data

This is not just a copying of all available SSTables into the Cassandra data directory -- we 
keep track of what the current set of SSTables are when we created the backup, meaning we only 
restore the minimum set of SSTables needed.

Another feature this enables is the ability to prune old backups accurately, based on what is
actually needed.

Example pruning backups older than 21 days:

    bin/cassandra-syncer-prune --older-than 21

# Installation

    # TODO: this isn't available on NPM yet (!)
    npm install -g cassandra-syncer

# Status

* First pass of syncer is working

# TODO

* Restore utility
* fsck utility
