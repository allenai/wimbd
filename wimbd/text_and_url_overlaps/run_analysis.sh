datasets_json=$1
intermediate_dir=$2
mode=$3

# enforce that mode is either "url" or "text"
if [ "$mode" != "url" ] && [ "$mode" != "text" ]; then
    echo "mode must be either 'url' or 'text'"
    exit 1
fi

# if mode is "url", then set the appropriate variables
if [ "$mode" == "url" ]; then
    mode_dir="url_overlaps"
    mode_script="run_get_urls.sh"
    mode_examples="--count_url_file"
fi

# if mode is "text", then set the appropriate variables
if [ "$mode" == "text" ]; then
    mode_dir="text_overlaps"
    mode_script="run_get_text_hashes.sh"
    mode_examples="--count_hash_file"
fi

## comments below refer to hashes, but only text uses hashes, urls use urls

# check that $TEXT_AND_URL_OVERLAPS is set

if [ -z "$TEXT_AND_URL_OVERLAPS" ]; then
    echo "TEXT_AND_URL_OVERLAPS must be set to the path of the text_and_url_overlaps directory"
    exit 1
fi

# report datasets to be processed
echo "Datasets to be processed:"
names_and_shard_paths=$(cat $datasets_json | jq -r 'keys[] as $k | "\($k) \(.[$k])" ')
echo "$names_and_shard_paths"


# mkdir -p $intermediate_dir/tmp/
# export TMPDIR=$intermediate_dir/tmp/

# make dir sorted_uniq_counts in intermediate_dir if not there already
mkdir -p $intermediate_dir/$mode_dir/sorted_uniq_counts

# 1) Get the hashes and remove duplicates via sort uniq.

# First get sorted unique counts of hashes in each of your datasets in one directory as follow
# use first arg to get data and second to set output 
echo "Getting sorted unique counts"
time parallel --colsep ' ' "bash $TEXT_AND_URL_OVERLAPS/helper_scripts/$mode_script {2} > $intermediate_dir/$mode_dir/sorted_uniq_counts/{1}.txt" ::: "${names_and_shard_paths[@]}"


# 2) Build lists of overlapping hashes from single datasets up to all datasets combined

# First we remove the counts to get the hash lists for each dataset by itself
mkdir -p $intermediate_dir/$mode_dir/sorted_uniq
for f in `ls $intermediate_dir/$mode_dir/sorted_uniq_counts`
do 
    $TEXT_AND_URL_OVERLAPS/helper_scripts/remove_count.sh $intermediate_dir/$mode_dir/sorted_uniq_counts/$f > $intermediate_dir/$mode_dir/sorted_uniq/$f
done

# Then we can merge those sorted hash lists to get overlaps as follows:
echo "Getting overlaps"
time python $TEXT_AND_URL_OVERLAPS/helper_scripts/run_combinations.py --sorted_uniq_dir $intermediate_dir/$mode_dir/sorted_uniq/ --datasets `cat $datasets_json | jq -r keys[]`

# 3) count the duplicates

# First get the size of the overlaps:
for f in `ls $intermediate_dir/$mode_dir/sorted_uniq/`
do
    wc -l $intermediate_dir/$mode_dir/sorted_uniq//$f >> $intermediate_dir/$mode_dir/overlap_numbers.txt
done

# count the duplicates
echo "Counting duplicates"
time python $TEXT_AND_URL_OVERLAPS/helper_scripts/get_duplicate_counts.py --sorted_uniq_count_dir $intermediate_dir/$mode_dir/sorted_uniq_counts/ --datasets `cat $datasets_json | jq -r keys[]` --out_file $intermediate_dir/$mode_dir/duplicate_counts.json


# 4) get top duplicate examples

# get top occurring duplicates
echo "Getting top duplicate examples"
mkdir -p $intermediate_dir/$mode_dir/sorted_sorted_uniq_counts
time parallel --eta --bar "sort -r {} > $intermediate_dir/$mode_dir/sorted_sorted_uniq_counts/{/}" ::: $intermediate_dir/$mode_dir/sorted_uniq_counts/*

# get top duplicate examples
mkdir -p $intermediate_dir/$mode_dir/top_examples
time parallel --eta --bar --colsep ' ' "bash $TEXT_AND_URL_OVERLAPS/helper_scripts/run_get_examples.sh {2} $intermediate_dir/$mode_dir/top_examples/{1}.jsonl $mode_examples $intermediate_dir/$mode_dir/sorted_sorted_uniq_counts/{1}.txt 30" ::: "${names_and_shard_paths[@]}"