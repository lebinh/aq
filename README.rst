=================================
aq - Query AWS resources with SQL
=================================

``aq`` allows you to query your AWS resources (EC2 instances, S3 buckets, etc.) with plain SQL.

.. image:: https://asciinema.org/a/79468.png
    :target: https://asciinema.org/a/79468

*But why?*
Fun, mostly fun. But see sample queries below for useful queries that can be performed with ``aq``.

Usage
~~~~~
::

    Usage:
        aq [options]
        aq [options] <query>

    Options:
        --table-cache-ttl=<seconds>  number of seconds to cache the tables
                                     before we update them from AWS again [default: 300]
        -v, --verbose  enable verbose logging

Running ``aq`` without specifying any query will start a REPL to run your queries interactively.

Sample queries
~~~~~~~~~~~~~~

One of the most important benefit of being able to query which SQL is aggregation and join,
which can be very complicated or even impossible to do with AWS CLI.

To count how many running instances per instance type
-----------------------------------------------------
::

    > SELECT instance_type, count(*) count
      FROM ec2_instances
      WHERE state->'Name' = 'running'
      GROUP BY instance_type
      ORDER BY count DESC
    +-----------------+---------+
    | instance_type   |   count |
    |-----------------+---------|
    | m4.2xlarge      |      15 |
    | m4.xlarge       |       6 |
    | r3.8xlarge      |       6 |
    +-----------------+---------+

Find instances with largest attached EBS volumes size
-----------------------------------------------------
::

    > SELECT i.id, i.tags->'Name' name, count(v.id) vols, sum(v.size) size, sum(v.iops) iops
      FROM ec2_instances i
      JOIN ec2_volumes v ON v.attachments -> 0 -> 'InstanceId' = i.id
      GROUP BY i.id
      ORDER BY size DESC
      LIMIT 3
    +------------+-----------+--------+--------+--------+
    | id         | name      |   vols |   size |   iops |
    |------------+-----------+--------+--------+--------|
    | i-12345678 | foo       |      4 |   2000 |   4500 |
    | i-12345679 | bar       |      2 |    332 |   1000 |
    | i-12345687 | blah      |      1 |    320 |    960 |
    +------------+-----------+--------+--------+--------+

Find instances that allows access to port 22 in their security groups
---------------------------------------------------------------------
::

    > SELECT i.id, i.tags->'Name' name, sg.group_name
      FROM ec2_instances i
      JOIN ec2_security_groups sg ON instr(i.security_groups, sg.id)
      WHERE instr(sg.ip_permissions, '"ToPort": 22,')
    +------------+-----------+---------------------+
    | id         | name      | group_name          |
    |------------+-----------+---------------------|
    | i-foobar78 | foobar    | launch-wizard-1     |
    | i-foobar87 | blah      | launch-wizard-2     |
    +------------+-----------+---------------------+

AWS Credential
~~~~~~~~~~~~~~

``aq`` relies on ``boto3`` for AWS API access so all the
`credential configuration mechanisms<https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration>`_
of boto3 will work. If you are using the AWS CLI then you can use ``aq`` without any further configurations.

Available tables
~~~~~~~~~~~~~~~~

AWS resources are specified as table names in ``<resource>_<collection>`` format with:

 resource
    one of the `resources <https://boto3.readthedocs.io/en/latest/guide/resources.html>`_
    defined in boto3: ``ec2``, ``s3``, ``iam``, etc.
 collection
    one of the resource's `collections <https://boto3.readthedocs.io/en/latest/guide/collections.html>`_
    defined in boto3: ``instances``, ``images``, etc.

An optional schema (i.e. database) name can be used to specify the AWS region to query.
If you don't specify the schema name then boto's default region will be used.

::

    -- to count the number of ec2 instances in AWS Singapore region
    SELECT count(*) FROM ap_southeast_1.ec2_instances

Note that the region name is specified using underscore (``ap_southeast_1``) instead of dash (``ap-southeast-1``).

At the moment the full table list for AWS ``us_east_1`` region is

.. list-table::

  * - cloudformation_stacks
  * - cloudwatch_alarms
  * - cloudwatch_metrics
  * - dynamodb_tables
  * - ec2_classic_addresses
  * - ec2_dhcp_options_sets
  * - ec2_images
  * - ec2_instances
  * - ec2_internet_gateways
  * - ec2_key_pairs
  * - ec2_network_acls
  * - ec2_network_interfaces
  * - ec2_placement_groups
  * - ec2_route_tables
  * - ec2_security_groups
  * - ec2_snapshots
  * - ec2_subnets
  * - ec2_volumes
  * - ec2_vpc_addresses
  * - ec2_vpc_peering_connections
  * - ec2_vpcs
  * - glacier_vaults
  * - iam_groups
  * - iam_instance_profiles
  * - iam_policies
  * - iam_roles
  * - iam_saml_providers
  * - iam_server_certificates
  * - iam_users
  * - iam_virtual_mfa_devices
  * - opsworks_stacks
  * - s3_buckets
  * - sns_platform_applications
  * - sns_subscriptions
  * - sns_topics
  * - sqs_queues

Query with structured value
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Quite a number of resource contain structured value (e.g. instance tags) that cannot be use directly in SQL.
We keep and present these values as JSON serialized string and add a new operator ``->`` to make querying on them easier.
The ``->`` (replaced to ``json_get`` before execution) can be used to access an object field, ``object->'fieldName'``, or access
an array item, ``array->index``::

    > SELECT '{"foo": "bar"}' -> 'foo'
    +-------------------------------------+
    | json_get('{"foo": "bar"}', 'foo')   |
    |-------------------------------------|
    | bar                                 |
    +-------------------------------------+
    > SELECT '["foo", "bar", "blah"]' -> 1
    +--------------+
    | json_get('   |
    |--------------|
    | bar          |
    +--------------+

Install
~~~~~~~
::

    pip install aq

Tests (with `nose`)
~~~~~~~~~~~~~~~~~~~
::

    nosetests
