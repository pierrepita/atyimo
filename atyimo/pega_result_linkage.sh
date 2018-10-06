#!/bin/bash
nf_temp='/scratch/robespierre.pita/entregas/baseline_sinanHANS/standardized/nf_temp'
nf_final='/scratch/robespierre.pita/entregas/baseline_sinanHANS/standardized/nf_final'
cp -fv /scratch/robespierre.pita/entregas/baseline_sinanHANS/linkage_results/result.linkage /scratch/robespierre.pita/entregas/baseline_sinanHANS/standardized/nf_temp
c=1
for x in `cat /scratch/robespierre.pita/entregas/baseline_sinanHANS/linkage_results/result.linkage | cut -d ';' -f3`; do
	if [ $(($c%2)) -eq '0' ]; then
		grep -v ^$x; $nf_temp > $nf_final;
	else
		grep -v ^$x; $nf_final > $nf_temp;
	c=`expr $c + 1`;
	fi
done
			
	
