``lamindb-airflow``
===================

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Basics

    Home <self>
    Changelog <changelog>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/lamindb>
    Lineage <lineage>
    Triggers <triggers>
    Sensors <sensors>
    Filters <filters>

Provider package for `LaminDB <https://lamin.ai>`__. It records DAG runs in LaminDB and lets DAGs react to
changes in LaminDB instances hosted on LaminHub:

* **Lineage**: record DAG runs as LaminDB flow runs and tasks as steps, with lamindb running in a
  virtualenv or a Kubernetes pod. See :doc:`lineage`.
* **Event-driven scheduling**: run DAGs when artifacts or records of any registry are created,
  updated or deleted, when branches (Change Requests) change their status, or when comments and readmes
  are added to branches. See :doc:`triggers`.
* **Deferrable sensors**: wait inside a DAG until a branch is merged or an artifact is available.
  See :doc:`sensors`.
* **Hook**: query the LaminHub REST API from tasks.
* **Filters**: select records with Python expressions such as
  ``F(ArtifactField.KEY).startswith("raw/")``, with enums for registries, fields and operators.
  See :doc:`filters`.

Requirements
------------

The minimum Apache Airflow version supported by this provider is ``3.3.0``, which introduced the asset
state store that the event triggers use to resume after restarts.

=======================================  ==================
PIP package                              Version required
=======================================  ==================
``apache-airflow``                       ``>=3.3.0``
``apache-airflow-providers-standard``
``httpx``                                ``>=0.27.0``
=======================================  ==================

The worker doesn't need the ``lamindb`` Python package. The triggers, sensors and hook talk to the
`LaminHub REST API <https://docs.lamin.ai/rest>`__, and the lineage operators install ``lamindb-core`` in a
virtualenv or use a Kubernetes pod's image. ``@task.lamindb_k8s`` needs
``pip install "lamindb-airflow[cncf.kubernetes]"``.

Installation
------------

.. code-block:: bash

    pip install lamindb-airflow
