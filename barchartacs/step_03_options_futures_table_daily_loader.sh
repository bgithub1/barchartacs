# run step_03_options_futures_table_daily_loader.py from the command line
#
# example with dashrisk3 as Virtualenv using local config and do not write csv file to postgres
# $ bash step_03_options_futures_table_daily_loader.sh ~/Virtualenvs3/dashrisk3  local False
#
# example with dashrisk3 as Virtualenv using secdb_aws config and DO write csv file to postgres
# $ bash step_03_options_futures_table_daily_loader.sh ~/Virtualenvs3/dashrisk3  secdb_aws True
#
virtualenv_folder=${1}
config_name=${2}
write_to_postgres=${3}

if [[ -z ${write_to_postgres} ]]
then
    write_to_postgres="False"
fi

if [[ -z ${virtualenv_folder} ]]
then
    virtualenv_folder="~/Virtualenvs3/dashrisk3"
fi


if [[ -z ${config_name} ]]
then
    config_name="local"
fi


source ${virtualenv_folder}/bin/activate

python3 step_03_options_futures_table_daily_loader.py --config_name local --write_to_postgres False

