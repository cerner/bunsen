# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = bunsen
SOURCEDIR     = source
BUILDDIR      = build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

.PHONY: deploy
deploy:
	git add . ; git checkout gh-pages
	cp -r $(BUILDDIR)/html/* ../;
	git add -u; git commit -m "Rebuilt and deployed docs"; git push origin gh-pages

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
		@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
	mvn -f ../bunsen-core/pom.xml package -DskipTests=true
	mkdir -p $(BUILDDIR)/html/core
	cp -r ../bunsen-core/target/apidocs $(BUILDDIR)/html/core/apidocs
	mvn -f ../bunsen-stu3/pom.xml package -DskipTests=true
	mkdir -p $(BUILDDIR)/html/stu3
	cp -r ../bunsen-stu3/target/apidocs $(BUILDDIR)/html/stu3/apidocs
	mkdir -p $(BUILDDIR)/html/r4
	mvn -f ../bunsen-r4/pom.xml package -DskipTests=true
	cp -r ../bunsen-r4/target/apidocs $(BUILDDIR)/html/r4/apidocs
