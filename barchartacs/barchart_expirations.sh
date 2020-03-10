commod=${1}
yy=${2}
echo 'symbol,expiry' > expiration_data/commod_expirations_${commod}${yy}.csv
for m in  F G H J K M N Q U V X Z;do curl https://www.barchart.com/futures/quotes/${commod}${m}${yy}/options|egrep -io "to expiration on".+strong.[0-1][0-9]/[0-3][0-9]/2[0-9]..strong|echo ${commod}${m}${yy},$(egrep -o "[0-1][0-9]/[0-3][0-9]/2[0-9]");done >> expiration_data/commod_expirations_${commod}${m}${yy}.csv
cat expiration_data/commod_expirations_${commod}${yy}.csv

