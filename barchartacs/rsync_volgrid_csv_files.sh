#
# send df_iv_final csv file to the server which publically runs volgrid
# the parameter sh_path is an sh script that executes either scp or rsync
#   in order to send files to a remote server.  The sh script must contain 3 arguments:
# arg1: full path of source file to send to the server
# arg2: full path on server of the workspace that holds the volgrid project
# arg3: either the word "live" or any other word, like "test".  
#    if this arg3 word is "live", then your sh_script should actually perform the rsync.
# 
# example:  use defaults (test mode, so nothing is written to server)
# bash rsync_volgrid_csv_files.sh
#
# example:  live  mode, write  to server
# bash rsync_volgrid_csv_files.sh live
#
# first get full path of volgrid/volgrid folder on remote server by executing a find on the word volgrid, and an egrep on volgrid/volgrid
ssh -i temp_folder/remote_pem.pem -t bitnami@$(cat temp_folder/remote_ip.txt) 'echo $(find $(pwd) -iname volgrid|egrep -o .+/volgrid/volgrid)' > temp_folder/remote_output.txt

# put the server path of volgrid/volgrid into the variable remote_volgrid
remote_volgrid=$(cat  temp_folder/remote_output.txt|egrep -o  "[/a-zA-Z0-9]+")
echo remote volgrid folder = ${remote_volgrid}

# determine if we are executing the rsync in test mode or live mode
# if you dont set rsync_mode to "live", then any other word will end up executing in test mode
rsync_mode=${1}
if [[ -z ${rsync_mode} ]]
then
   rsync_mode="test"
fi

find "$(pwd)" -maxdepth 2 -iname "df_iv_final_*.csv"|while read -r l;do echo 'bash rsync_to_remote.sh ' ${l} ${remote_volgrid} ${rsync_mode};done > rsync_temp_sh.sh 
bash rsync_temp_sh.sh

