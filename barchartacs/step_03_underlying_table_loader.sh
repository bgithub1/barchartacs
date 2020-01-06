# run step_03_underlying_table_loader.py from the command line
#
# example with postgres username and password, multiple years, using the "local" config in postgres_info.csv::
# $ bash step_03_underlying_table_loader.sh 2016 2019 zip_folder_parent True virtualenv_folder local 
#
# example with postgres username and password, single year and 2 months (oct,nov), using the "local" config in postgres_info.csv::
# $ bash step_03_underlying_table_loader.sh 2019 2019 zip_folder_parent True virtualenv_folder local "oct,nov"
#
# example same as above with a contract_list
# $ bash step_03_underlying_table_loader.sh 2019 2019 zip_folder_parent True virtualenv_folder local "oct,nov" "CL,NG" 
#
# example DON'T write to postgres, only creating csv file for multiple years which 
#    will be uploaded using the psql COPY command that get's printed at 
#    the end of the run), using the "aws_lightsail" config in postgres_info.csv:
# $ bash step_03_underlying_table_loader.sh 2016 2019 zip_folder_parent False virtualenv_folder aws_lightsail 
#
begin_yyyy=${1}
end_yyyy=${2}
zip_folder_parent=${3}
write_to_postgres=${4}
virtualenv_folder=${5}
config_name=${6}
months_to_include="${7}"
contract_list="${8}"

if [[ -z ${write_to_postgres} ]]
then
    write_to_postgres="False"
fi

if [[ -z ${virtualenv_folder} ]]
then
    virtualenv_folder="~/Virtualenvs3/dashrisk2"
fi

if [[ -z ${months_to_include} ]]
then
    months_to_include="jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec"
fi

if [[ -z ${config_name} ]]
then
    config_name="local"
fi

if [[ -z ${contract_list} ]]
then
    contract_list="CL,CB,ES,NG"
fi

source ${virtualenv_folder}/bin/activate

python3 step_03_underlying_table_loader.py --write_to_postgres ${write_to_postgres} --zip_folder_parent ${zip_folder_parent}  --begin_yyyy ${begin_yyyy} --end_yyyy ${end_yyyy} --config_name ${config_name}  --months_to_include ${months_to_include} --contract_list ${contract_list}

