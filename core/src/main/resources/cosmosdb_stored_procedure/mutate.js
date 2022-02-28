function mutate(num, ...args) {
  let mutations = args.slice(0, num);
  let baseRecords = args.slice(num, num * 2);
  let queries = args.slice(num * 2);

  const MUTATION = {
    PUT: 0,
    PUT_IF_NOT_EXISTS: 1,
    PUT_IF: 2,
    DELETE_IF: 3,
  };
  const ERROR_CODE = {
    PRECONDITION_FAILED: 412,
  };

  function merge(source, update) {
    for (var column in update) {
      if (update[column] == null) {
        delete source[column];
      } else if (update[column].constructor == Object) {
        source[column] = merge(source[column], update[column]);
      } else {
        source[column] = update[column];
      }
    }

    return source;
  }

  function mutateWithQuery(mutation, record, query) {
    return new Promise((resolve, reject) => {
      const isAccepted = __.queryDocuments(__.getSelfLink(), query,
        (error, reads, options) => {
          if (error) {
            reject(error);
            return;
          }

          const records = checkQueryResult(mutation, record, reads);
          if (!records.length) {
            reject(new Error(ERROR_CODE.PRECONDITION_FAILED, "no mutation"));
            return;
          }

          if (mutation == MUTATION.DELETE_IF) {
            for (let i = 0; i < records.length; i++) {
              const isMutationAccepted = __.deleteDocument(records[i]._self,
                (error, result, options) => {
                  if (error) {
                    reject(error);
                  } else {
                    resolve(result);
                  }
                });
              if (!isMutationAccepted) {
                reject(new Error("The query was not accepted by the server."));
              }
            }
          } else {
            const isMutationAccepted = __.upsertDocument(__.getSelfLink(), records[0],
              (error, result, options) => {
                if (error) {
                  reject(error);
                } else {
                  resolve(result);
                }
              });
            if (!isMutationAccepted) {
              reject(new Error("The query was not accepted by the server."));
            }
          }
        });
      if (!isAccepted) reject(new Error("The query was not accepted by the server."));
    });
  }

  function deleteNullValues(record) {
    for (var column in record) {
      if (record[column] == null) {
        delete record[column];
      } else if (record[column].constructor == Object) {
        deleteNullValues(record[column]);
      }
    }
    return record;
  }

  function checkQueryResult(mutation, record, reads) {
    if (!reads || !reads.length) {
      // The specified record does not exist
      if (mutation == MUTATION.PUT || mutation == MUTATION.PUT_IF_NOT_EXISTS) {
        return [deleteNullValues(record)];
      } else {
        // For other conditional mutations, the condition isn't satisfied
        return [];
      }
    }

    if (mutation == MUTATION.PUT_IF_NOT_EXISTS) {
      return [];
    } else if (mutation == MUTATION.PUT || mutation == MUTATION.PUT_IF) {
      if (reads.length != 1) {
        throw new Error("Unable to update multiple records.");
      } else {
        return [merge(reads[0], record)];
      }
    } else {
      // DELETE_IF
      return reads;
    }
  }

  let promises = [];
  for (let i = 0; i < num; i++) {
    let mutation = mutations[i];
    let record = baseRecords[i];
    let query = queries[i];

    promises.push(mutateWithQuery(mutation, record, query));
  }

  Promise.all(promises).catch(error => getContext().abort(error));
}
