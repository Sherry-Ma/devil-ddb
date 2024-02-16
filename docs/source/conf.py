# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# DDB custom:
import sys
import os
import toml

rootdir = os.path.abspath(os.path.join('..', '..'))
srcdir = os.path.join(rootdir, 'src')
sys.path.insert(0, srcdir) # needed for autodoc to find module root
poetry_config = toml.load(os.path.join(rootdir, 'pyproject.toml'))

extlinks = {
    'projurl': (poetry_config['tool']['ddb']['url'] + '%s', None),
    'docurl': (poetry_config['tool']['ddb']['docbaseurl'] + '%s', None),
    'codeurl': (poetry_config['tool']['ddb']['codebaseurl'] + '%s', None),
    'rawcodeurl': (poetry_config['tool']['ddb']['rawcodebaseurl'] + '%s', None),
}

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = poetry_config['tool']['poetry']['name']
copyright = '2023, {}'.format(str(poetry_config['tool']['poetry']['authors']))
author = str(poetry_config['tool']['poetry']['authors'])

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.coverage',
    'sphinx.ext.mathjax',
    'sphinx.ext.extlinks',
]

templates_path = ['_templates']
exclude_patterns = []

autodoc_default_options = {
    'member-order': 'bysource',
    'undoc-members': True,
    'special-members': '__init__, __enter__, __exit__',
    'private-members': True
}

nitpicky = True
nitpick_ignore = [
    ('py:class', 'builtins.list'),
    ('py:class', 'enum.Enum'),
    ('py:class', 'abc.ABC'),
    ('py:class', 'abc.ABCMeta'),
    ('py:class', 'collections.OrderedDict'),
    ('py:class', 'queue.PriorityQueue'),
    ('py:class', 'sqlglot.expressions.Expression'),
    ('py:class', 'sqlglot.expressions.Create'),
    ('py:class', 'sqlglot.expressions.Select'),
    ('py:class', 'sqlglot.expressions.Insert'),
    ('py:class', 'sqlglot.expressions.Command'),
    ('py:class', 'sqlglot.expressions.Delete'),
    ('py:class', 'sqlglot.expressions.Values'),
    ('py:class', 'sqlglot.expressions.DataType.Type'),
    ('py:class', 'lmdb.Transaction'),
    ('py:class', 'lmdb.cffi.Transaction'),
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']
