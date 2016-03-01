# Get all crashreports including reports spreading multiple lines

BEGIN {
    crashreport=0
    timestamp_pattern="^20[[:digit:]]{2}-[[:digit:]]{2}-[[:digit:]]{2}"
}

# FILENAME == last_filename { last_filename=FILENAME}
# FILENAME != last_filename { last_filename=FILENAME; print FILENAME}

crashreport == 1 && $0 ~ timestamp_pattern { crashreport = 0; print ""} # leave crash report
crashreport == 0 && /FD/ { crashreport = 1; print $0 } # enter crash report
crashreport == 1 && $0 !~ timestamp_pattern { print "\t", $0 } # during crash report
