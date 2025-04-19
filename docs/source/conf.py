"""Configuration file for the Sphinx documentation builder.

For the full list of built-in configuration values, see the documentation:
https://www.sphinx-doc.org/en/master/usage/configuration.html

-- Project information -----------------------------------------------------
https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
"""

from time import strftime

project = 'Slipstream'
html_title = project
author = 'Menziess'
copyright = f'{strftime("%Y")}, {author}'  # noqa: A001

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

templates_path = ['_templates']
exclude_patterns = ['*_build', '*Thumbs.db', '*.DS_Store', '*.venv/*']
extensions = [
    'sphinx.ext.viewcode',
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autosectionlabel',
    'autoapi.extension',
]

autosectionlabel_prefix_document = True

autoapi_python_path = 'slipstream'
autoapi_dirs = ['../../slipstream']
autoapi_ignore = exclude_patterns
autoapi_type = 'python'
autoapi_template_dir = f'{templates_path[0]}/autoapi'
autoapi_keep_files = True
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
