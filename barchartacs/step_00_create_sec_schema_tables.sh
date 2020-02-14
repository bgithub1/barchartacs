# run build_db.py to build the sec_schema schema and the tables options_table and futures_table
#
# example with postgres username and password and default virtual env
# $ bash step_00_create_tables.sh db_username db_password 
#
# example with postgres username and password and specify virtual env as myvirtual_env
# $ bash step_00_create_tables.sh db_username db_password myvirtual_env 
#

virtualenv_folder=${1}
config_name=${2}
testorlive=${3}


if [[ -z ${virtualenv_folder} ]]
then
    virtualenv_folder="~/Virtualenvs3/dashrisk2"
fi


if [[ -z ${config_name} ]]
then
    config_name="local"
fi

if [[ -z ${testorlive} ]]
then
    testorlive="test"
fi


source ${virtualenv_folder}/bin/activate
python3 step_00_create_sec_schema_tables.py --testorlive ${testorlive} --config_name ${config_name}
