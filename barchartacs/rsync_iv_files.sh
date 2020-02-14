# sync df_files for skew analysis to folders on remote bitnami
# example: do test run
# bash rsync_iv_files.sh
# example: do live run
# bash rsync_iv_files.sh live
# 
test_or_live=${1}
if [[ -z {test_or_live} ]]
then
   ${test_or_live}_or_live=test
fi
#bash rsync_to_remote.sh 'temp_folder/df_*_CL.csv' /home/bitnami/pyliverisk/volgrid/volgrid ${test_or_live}
bash rsync_to_remote.sh 'temp_folder/df_*_CL.csv' /home/bitnami/pyliverisk/volgrid/volgrid ${test_or_live}
bash rsync_to_remote.sh 'temp_folder/df_*_CB.csv' /home/bitnami/pyliverisk/volgrid/volgrid ${test_or_live}
bash rsync_to_remote.sh 'temp_folder/df_*_ES.csv' /home/bitnami/pyliverisk/volgrid/volgrid ${test_or_live}
bash rsync_to_remote.sh 'temp_folder/df_*_NG.csv' /home/bitnami/pyliverisk/volgrid/volgrid ${test_or_live}
bash rsync_to_remote.sh 'temp_folder/df_*_CL.csv' /home/bitnami/pyliverisk/dashgrid/dashgrid ${test_or_live}
bash rsync_to_remote.sh 'temp_folder/df_*_CB.csv' /home/bitnami/pyliverisk/dashgrid/dashgrid ${test_or_live}
bash rsync_to_remote.sh 'temp_folder/df_*_ES.csv' /home/bitnami/pyliverisk/dashgrid/dashgrid ${test_or_live}
bash rsync_to_remote.sh 'temp_folder/df_*_NG.csv' /home/bitnami/pyliverisk/dashgrid/dashgrid ${test_or_live}
