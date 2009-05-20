# warnings einfärben
/Warning/ { print "\033[1;34m" $0 "\033[0m" }
/Error/ { print "\033[1;31m" $0 "\033[0m" }
/undefined/ { print "\033[1;32m" $0 "\033[0m" }
/Overfull/ { print "\033[1;36m" $0 "\033[0m" }
/Underfull/ { print "\033[1;36m" $0 "\033[0m" }
# geladene Files verbergen
#/^\(/ {}
#/pipe/ {}
# den Rest ausgeben
$0 !~ /Warning|bibitem|duplicate|Overfull|Underfull|Error|Undefined|document|^$|inconsistent|pdfTeX|\[\]|^\(/ { print }
