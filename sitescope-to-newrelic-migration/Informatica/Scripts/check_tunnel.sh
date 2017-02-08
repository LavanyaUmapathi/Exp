if pgrep -f "ssh -L 9998:localhost:10000 admin1.prod.iad.caspian.rax.io -f -N -l ebi_informatica" > /dev/null
then
  echo "Running"
else
  echo "Not Running"
  cmd="ssh -L 9998:localhost:10000 admin1.prod.iad.caspian.rax.io -f -N -l ebi_informatica"
  eval $cmd
fi
