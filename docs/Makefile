# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
AUTOOPTS      ?=
AUTOBUILD     ?= sphinx-autobuild
APIDOCOPTS    ?= -M -d 2 -f
APIDOCBUILD   ?= sphinx-apidoc
SOURCEDIR     = source
BUILDDIR      = _build

auto:
	@$(APIDOCBUILD) -o "$(SOURCEDIR)" ../slipstream $(APIDOCOPTS)
	@$(AUTOBUILD) "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help auto Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(APIDOCBUILD) -o "$(SOURCEDIR)" ../slipstream $(APIDOCOPTS)
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
