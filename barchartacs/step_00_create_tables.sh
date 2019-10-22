# run build_db.py to build the sec_schema schema and the tables options_table and futures_table
#
# example with postgres username and password and default virtual env
# $ bash step_00_create_tables.sh db_username db_password 
# example with postgres username and password and specify virtual env as myvirtual_env
# $ bash step_00_create_tables.sh db_username db_password myvirtual_env 
#

username=${1}
password=${2}
virtualenv_folder=${3}

if [[ -z ${virtualenv_folder} ]]
then
    virtualenv_folder="~/Virtualenvs3/dashrisk2"
fi

echo run the following commands from the console
echo source ${virtualenv_folder}/bin/activate
echo python3 build_db.py  --username ${username} --password ${password} --recreate_schema True --recreate_tables True
