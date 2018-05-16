# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = bunsen
SOURCEDIR     = source
BUILDDIR      = build

# Grab the project version from Maven.
PROJECT_VERSION := $(shell mvn -f ../pom.xml --quiet \
                               -Dexec.executable="echo" \
								  		         -Dexec.args='$${project.version}' \
								 							 --non-recursive \
															 exec:exec)

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

.PHONY: deploy
deploy:

	@echo Deploying documentation for $(PROJECT_VERSION)

	# Create an HTML page to redirect to the newly built version.
	sed s/PROJECT_VERSION/$(PROJECT_VERSION)/g redirect.html > $(BUILDDIR)/redirect.html

	git add . ; git checkout gh-pages
	cp -r $(BUILDDIR)/html ../$(PROJECT_VERSION)
	cp $(BUILDDIR)/redirect.html ../index.html

	git add ../index.html ; git add ../$(PROJECT_VERSION)
	git commit -m "Add docs for $(PROJECT_VERSION)" ; git push origin gh-pages

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
		@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) \
		 -D version=$(PROJECT_VERSION) -D release=$(PROJECT_VERSION) $(O)
	mvn -f ../pom.xml site -DskipTests=true
	mkdir -p $(BUILDDIR)/html
	cp -r ../target/site/apidocs $(BUILDDIR)/html/apidocs
