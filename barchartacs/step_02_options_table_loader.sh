# run step_02_options_table_loader.py from the command line
#
# example with postgres username and password, multiple years, using the "local" config in postgres_info.csv::
# $ bash step_02_options_table_loader.sh 2016 2019 zip_folder_parent True virtualenv_folder local 
#
# example DON'T write to postgres, only creating csv file for multiple years which 
#    will be uploaded using the psql COPY command that get's printed at 
#    the end of the run), using the "aws_lightsail" config in postgres_info.csv:
# $ bash step_02_options_table_loader.sh 2016 2019 zip_folder_parent False virtualenv_folder aws_lightsail 
#

begin_yyyy=${1}
end_yyyy=${2}
zip_folder_parent=${3}
write_to_postgres=${4}
virtualenv_folder=${5}
config_name=${6}


if [[ -z ${write_to_postgres} ]]
then
    write_to_postgres="False"
fi

if [[ -z ${virtualenv_folder} ]]
then
    virtualenv_folder="~/Virtualenvs3/dashrisk2"
fi

source ${virtualenv_folder}/bin/activate

if [[ -z ${config_name} ]]
then
    python3 step_02_options_table_loader.py --write_to_postgres ${write_to_postgres} --zip_folder_parent ${zip_folder_parent}  --begin_yyyy ${begin_yyyy} --end_yyyy ${end_yyyy}
else
    python3 step_02_options_table_loader.py --write_to_postgres ${write_to_postgres} --zip_folder_parent ${zip_folder_parent}  --begin_yyyy ${begin_yyyy} --end_yyyy ${end_yyyy}  --config_name ${config_name} 
fi

