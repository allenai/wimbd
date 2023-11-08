URL=$1
LOCAL_DIR=$2

DIR="$(dirname $1)"
LANG="$(basename $DIR)"
FILENAME="$(basename $URL)"
JSONL_FILENAME=${FILENAME%.*}.jsonl.gz

DOWNLOAD_PATH=$LOCAL_DIR/$LANG/$JSONL_FILENAME

# DEST_EXISTS=$(aws s3 ls $dest_path)

DOWNLOAD_SIZE=$(ls -s $DOWNLOAD_PATH)
EMPTY_SIZE_STR='0 '$DOWNLOAD_PATH

if [[ "$DOWNLOAD_SIZE" == *"$EMPTY_SIZE_STR"* ]]
# if [[ ! -f $DOWNLOAD_PATH ]]
then
    # echo $SOURCE_PATH $DEST_PATH

    # Load the remote URL and save it as a jsonl.gz file under LOCAL_DIR
    python download_url.py $URL $LOCAL_DIR
fi
