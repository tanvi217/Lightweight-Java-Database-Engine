#!/bin/bash
# Usage: ./query.sh start_range end_range [buffer_size]

query1=("'Wisd'" "'Wist'" "'100'")
query2=("'Billy Bu'" "'Billz'" "'150'")
query3=("'Close Encounters'" "'Closer'" "'200'")
query4=("'Star Wars: Episode'" "'Star Wars: Episode Z'" "'99'")
query5=("'Cléo from 5 to 7'" "'Cléo from 5 to 7'" "'80'") # tt0055852
query6=("'la jetee'" "'La Jetee'" "'87'") # tt0056119
query7=("'8½'" "'8½'" "'120'") # tt0056801

if [ "$#" -eq 0 ]; then
    args="${query1[*]}"
elif [ "$#" -eq 1 ]; then
    if [ "$1" = "1" ]; then
        args="${query1[*]}"
    elif [ "$1" = "2" ]; then
        args="${query2[*]}"
    elif [ "$1" = "3" ]; then
        args="${query3[*]}"
    elif [ "$1" = "4" ]; then
        args="${query4[*]}"
    elif [ "$1" = "5" ]; then
        args="${query5[*]}"
    elif [ "$1" = "6" ]; then
        args="${query6[*]}"
    elif [ "$1" = "7" ]; then
        args="${query7[*]}"
    else
        args=("'$1'" "'$1'" "'200'")
    fi
elif [ "$#" -eq 2 ]; then
    args=("'$1'" "'$2'" "'200'")
elif [ "$#" -eq 4 ]; then
    args=("'$1'" "'$2'" "'$3'" "'$4'")
else
    args=("'$1'" "'$2'" "'$3'")
fi

echo "run_query ${args[*]}"

ant query-imdb -emacs -Dargs="${args[*]}"
