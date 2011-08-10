A tool for syncing Csasandra SSTables out to various destinations.

Example use saving to Cloudfiles:

    bin/cassandra-syncer --dest cloudfiles://user:password@/ --source /var/lib/cassandra/data

Example use saving to another directory, for example when you mount an EBS volume to backup the sstables:

    bin/cassandra-syncer --dest directory:///mnt/network-volume --source /var/lib/cassandra/data
