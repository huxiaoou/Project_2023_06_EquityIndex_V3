$bgn_date = "20150416"
$bgn_date_ma = "20160615"
$bgn_date_test = "20160701"
$stp_date = "20240201"

python main.py --switch preprocess --factor m01 --mode o --bgn $bgn_date --stp $stp_date
python main.py --switch preprocess --factor pub --mode o --bgn $bgn_date --stp $stp_date
python main.py --switch test_returns --mode o --bgn $bgn_date --stp $stp_date
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor amp
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor amt
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor basis
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor beta
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor cx
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor exr
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor mtm
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor pos
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor rng
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor sgm
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor size
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor skew
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor smt
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor to
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor ts
python main.py --switch factors_exposure --mode o --bgn $bgn_date --stp $stp_date --factor twc
python main.py --switch fema --mode o --bgn $bgn_date_ma   --stp $stp_date
python main.py --switch ic   --mode o --bgn $bgn_date_test --stp $stp_date
python main.py --switch icsum         --bgn $bgn_date_test --stp $stp_date

python main.py -mode o -bgn $bgn_date_ma -stp $stp_date -w sig
python main.py -mode o -bgn $bgn_date_test -stp $stp_date -w simu
python main.py --switch simusum
