"""
Configuration file for the Sphinx documentation builder.

For the full list of built-in configuration values, see the documentation:
https://www.sphinx-doc.org/en/master/usage/configuration.html

-- Project information -----------------------------------------------------
https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
"""
from time import strftime

project = 'slipstream'
copyright = f'{strftime("%Y")}, Menziess'
author = 'Menziess'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.viewcode',
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'autoapi.extension',
]

templates_path = ['_templates']
exclude_patterns = ['*_build', '*Thumbs.db', '*.DS_Store', '*.venv/*']
autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'show-inheritance': True,
}
autosummary_generate = True

autoapi_dirs = ['../../slipstream']
autoapi_ignore = exclude_patterns
autoapi_type = 'python'
autoapi_template_dir = '_templates/autoapi'
autoapi_keep_files = True
autodoc_typehints = 'signature'
autoapi_options = [
    'members',
    'undoc-members',
    'show-inheritance',
    'show-module-summary',
    'imported-members',
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
pygments_style = 'sphinx'
