# run build_db.py to build the sec_schema schema and the tables options_table and futures_table
#
# example with virtualenv=~/Virtualenvs3/dashrisk3, db config_name=secdb_aws test mode (don't create tables)
# $ bash step_00_create_tables.sh ~/Virtualenvs3/dashrisk3 secdb_aws test 
#
# example with virtualenv=~/Virtualenvs3/dashrisk3, db config_name=secdb_aws live mode (CREATE tables)
# $ bash step_00_create_tables.sh ~/Virtualenvs3/dashrisk3 secdb_aws live 
#

virtualenv_folder=${1}
config_name=${2}
testorlive=${3}


if [[ -z ${virtualenv_folder} ]]
then
    virtualenv_folder="~/Virtualenvs3/dashrisk3"
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
