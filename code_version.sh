#!/bin/bash

if [[ -f version.txt ]]; then
    version=$(cat version.txt)
    from_statement="from toplevel 'version' file"
else
    version=$(git rev-parse --short=12 HEAD)
    from_statement="from git hash (fallback method)"
fi

# Remove quotes from the version if present
version=${version//\"/}
version=${version//\'/}

echo "Parsed version $from_statement: '$version'" 1>&2

echo "$version"
