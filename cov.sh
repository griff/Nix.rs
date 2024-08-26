
#RUSTFLAGS="-C instrument-coverage" \
#    cargo test --tests

#llvm-profdata merge -sparse default_*.profraw -o json5format.profdata

llvm-cov show -format=html \
    --show-directory-coverage \
    $( \
      for file in \
        $( \
          RUSTFLAGS="-C instrument-coverage" \
            cargo test --tests --no-run --message-format=json \
              | jq -r "select(.profile.test == true) | .filenames[]" \
              | grep -v dSYM - \
        ); \
      do \
        printf "%s %s " -object $file; \
      done \
    ) \
  --ignore-filename-regex='/Users/brian/.cargo' \
  --instr-profile=json5format.profdata > cov.html