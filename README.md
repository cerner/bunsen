# _About_

Bunsen lets users load, transform, and analyze FHIR data with Apache Spark. It offers Java and Python APIs to convert FHIR resources into Spark Datasets, which then can be explored with the
full power of that platform, including with Spark SQL. For details see the [Bunsen documentation](http://cerner.github.io/bunsen/).

# _Building_

Bunsen is built and tested with [Apache Maven](https://maven.apache.org), with the standard Maven lifecycle to build, install, and deploy it.

User documentation is built with [Sphinx](http://www.sphinx-doc.org/en/stable/). PySpark should be installed in the environment to generate
the Python documentation. With that in place, the user can simply run ```make html``` in the docs directory to build the documentation,
and ```make deploy``` in that directory to publish it to the GitHub pages site.

# _Availability_

Bunsen is hosted in the [Maven Central](https://repo.maven.apache.org/maven2/com/cerner/) repository.

# _Conventions_

Bunsen's Java code should follow the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html).

# _Communication_

Please use GitHub issues to record any requests or issues for this project.

# Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

# LICENSE

Copyright 2017 Cerner Innovation, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

&nbsp;&nbsp;&nbsp;&nbsp;http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
