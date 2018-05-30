# Minimal makefile to create a Docker file

# Grab the project version from Maven.
PROJECT_VERSION := $(shell mvn -f ../pom.xml --quiet \
				-Dexec.executable="echo" \
				-Dexec.args='$${project.version}' \
				--non-recursive \
				exec:exec)

image:
	@echo Building Docker image for $(PROJECT_VERSION)

	# Unzip the assembly under the current folder so the Docker file can add it.
	unzip ../assembly/target/bunsen-assembly-$(PROJECT_VERSION)-dist.zip -d .

	docker build . --build-arg BUNSEN_VERSION=$(PROJECT_VERSION) -t cerner/bunsen-notebook

	# Clean up local copy of the assembly
	rm -r ./bunsen-assembly-$(PROJECT_VERSION)
