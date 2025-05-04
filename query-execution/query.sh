#!/bin/bash
# Usage: ./query.sh start_range end_range [buffer_size]

query1=("'Wisd'" "'Wist'" "'100'")
query2=("'Billy Bu'" "'Billz'" "'150'")
query3=("'Close Encounters'" "'Closer'" "'200'")

if [ "$#" -eq 1 ]; then
    if [ "$1" = "1" ]; then
        args="${query1[*]}"
    elif [ "$1" = "2" ]; then
        args="${query2[*]}"
    elif [ "$1" = "3" ]; then
        args="${query3[*]}"
    else
        args=("'$1'" "'$1'" "'200'")
    fi
elif [ "$#" -eq 2 ]; then
    args=("'$1'" "'$2'" "'200'")
else
    args=("'$1'" "'$2'" "'$3'")
fi

echo "run_query ${args[*]}"

ant query_imdb -Dargs="${args[*]}"
