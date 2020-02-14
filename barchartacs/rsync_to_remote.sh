# execute an rsync operation using 2 files in temp_folder:
#  1. the file temp_folder/remote_pem.pem which holds the pem of the destination server 
#  2. the file temp_folder/remote_ip.txt which holds an ip address (no new line characters) of the remote server
#
#example:
# bash rsync_to_remote.sh $(pwd)/my_file_to_send.txt /home/myremote_folder test 
source_path="$1"
dest_path="$2"
run_type=$3 # set this to 'live' if you want the actual transfer to take place
ipadd=$(cat temp_folder/remote_ip.txt)

username=$4
if [[ -z ${username} ]]
then
   username=bitnami
fi
if [[ ${run_type} == 'live' ]]
then
   rsync -ve "ssh -i temp_folder/remote_pem.pem" ${source_path}  ${username}@${ipadd}:"${dest_path}"
else
   rsync -nve "ssh -i temp_folder/remote_pem.pem" ${source_path} ${username}@${ipadd}:"${dest_path}"
fi
