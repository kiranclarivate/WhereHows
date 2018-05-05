!/bin/bash

usage(){
    echo "Usage:"
    echo "$0 file_path url_and_port url_path"
    echo "example:"
    echo "bin/upload_flow.sh json/example.json ec2-34-218-45-197.us-west-2.compute.amazonaws.com:8899 dataset"
    exit 1
}


if [[ $# -ne 3 ]] ; then
    usage
fi

echo "start uploading $1 to $2/$3"

if [[ $1 = *"csv/"* ]]; then
  echo "csv"
elif [[ $1 = *"yaml/"* ]]; then
    echo "yaml"
elif [[ $1 = *"json/"* ]]; then
    curl -H "Content-Type: application/json" -X POST -d "@$1" "$2/$3"
fi

