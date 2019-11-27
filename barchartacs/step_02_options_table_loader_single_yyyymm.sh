# run step_02_options_table_loader.py from the command line FOR A SINGLE yyyymm
#
# example with postgres username and password, multiple years:
# $ bash step_02_options_table_loader_single_yyyymm.sh 201910 zip_folder_parent True virtualenv_folder db_username db_password 
#
# example DON'T write to postgres, only creating csv file for multiple years which 
#    will be uploaded using the psql COPY command that get's printed at 
#    the end of the run)
# $ bash step_02_options_table_loader_single_yyyymm.sh 201910 zip_folder_parent False virtualenv_folder db_username db_password 
#

single_yyyymm=${1}
zip_folder_parent=${2}
write_to_postgres=${3}
virtualenv_folder=${4}
db_username=${5}
db_password=${6}


if [[ -z ${write_to_postgres} ]]
then
    write_to_postgres="False"
fi

if [[ -z ${virtualenv_folder} ]]
then
    virtualenv_folder="~/Virtualenvs3/dashrisk2"
fi

source ${virtualenv_folder}/bin/activate

if [[ -z ${db_username} ]]
then
    python3 step_02_options_table_loader.py --write_to_postgres ${write_to_postgres} --zip_folder_parent ${zip_folder_parent}  --single_yyyymm ${single_yyyymm}
else
    python3 step_02_options_table_loader.py --write_to_postgres ${write_to_postgres} --zip_folder_parent ${zip_folder_parent}  --single_yyyymm ${single_yyyymm}  --db_username ${db_username} --db_password ${db_password} 
fi

