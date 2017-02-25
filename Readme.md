# Readme

## Requirements
 - Python 3 (incl. pip & virtualenv)
 - pigz (faster than gzip)
 - gsutil
 - bq

```
virtualenv --python python3 env
source env/bin/activate
pip install -r requirements.txt
python migrate -d database -t table | split -d -C 4GB --filter='pigz > $FILE.gz' -a 4 - /path/to/tmp/dir
deactivate

gsutil -m cp /path/to/tmp/dir/files* gs://backet/path/to/
bq load --nosync --allow_quoted_newlines --max_bad_records=100 dataset.table gs://backet/path/to/files* schema.json
```
