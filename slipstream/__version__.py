"""Defines the package version."""

from os import getenv

VERSION = getenv('PACKAGE_VERSION', '0.0.dev0')
