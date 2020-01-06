# run step_02_options_table_loader.py from the command line FOR A SINGLE yyyymm
#
# example with postgres username and password, multiple years, using the "local" config in postgres_info.csv:
# $ bash step_02_options_table_loader_single_yyyymm.sh 201910 zip_folder_parent True virtualenv_folder local 
#
# example DON'T write to postgres, only creating csv file for multiple years which 
#    will be uploaded using the psql COPY command that get's printed at 
#    the end of the run), using the "local" config in postgres_info.csv:
# $ bash step_02_options_table_loader_single_yyyymm.sh 201910 zip_folder_parent False virtualenv_folder local 
#
#
# example DON'T write to postgres, only creating csv file for multiple years which 
#    will be uploaded using the psql COPY command that get's printed at 
#    the end of the run), using the "local" config in postgres_info.csv and
#    using 2 products, CL,NG as the contract_list:
# $ bash step_02_options_table_loader_single_yyyymm.sh 201910 zip_folder_parent False virtualenv_folder local CL,NG
#
# example: same as above but with 
#    using 1 product, NG as the contract_list:
# $ bash step_02_options_table_loader_single_yyyymm.sh 201910 zip_folder_parent False virtualenv_folder local NG
#
single_yyyymm=${1}
zip_folder_parent=${2}
write_to_postgres=${3}
virtualenv_folder=${4}
config_name=${5}
contract_list=${6}


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
    config_name="local"
fi

if [[ -z ${contract_list} ]]
then
    contract_list="CL,CB,ES,NG"
fi

# if [[ -z ${config_name} ]]
# then
#     python3 step_02_options_table_loader.py --write_to_postgres ${write_to_postgres} --zip_folder_parent ${zip_folder_parent}  --single_yyyymm ${single_yyyymm}
# else
#     python3 step_02_options_table_loader.py --write_to_postgres ${write_to_postgres} --zip_folder_parent ${zip_folder_parent}  --single_yyyymm ${single_yyyymm}  --config_name ${config_name} 
# fi

python3 step_02_options_table_loader.py --write_to_postgres ${write_to_postgres} --zip_folder_parent ${zip_folder_parent}  --single_yyyymm ${single_yyyymm}  --config_name ${config_name} --contract_list ${contract_list}

