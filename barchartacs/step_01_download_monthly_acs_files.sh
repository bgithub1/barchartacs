# run step_01_download_monthly_acs_files.py from the command line
# example:
# $bash step_01_download_monthly_acs_files.sh acs_username acs_password zip_folder_parent 11 19 barchartacs_virtualenv_folder 
#
acs_username=${1}
acs_password=${2}
begin_yy=${3}
end_yy=${4}
zip_folder_parent=${5}
virtualenv_folder=${6}

if [[ -z ${zip_folder_parent} ]]
then
    zip_folder_parent="./temp_folder/zip_files"
fi

if [[ -z ${virtualenv_folder}]]
then
    virtualenv_folder="~/Virtualenvs3/dashrisk2"
fi

source ${virtualenv_folder}/bin/activate
python3 step_01_download_monthly_acs_files.py --acs_username ${acs_username{} --acs_password ${acs_password} --begin_yy ${begin_yy}  --end_yy ${end_yy} --zip_folder_parent ${zip_folder_parent} 
