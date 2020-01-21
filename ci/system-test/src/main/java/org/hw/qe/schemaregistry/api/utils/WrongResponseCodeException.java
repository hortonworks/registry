/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api.utils;

import org.hw.qe.requests.BaseResponse;

/**
 * Created by sbhat on 8/29/17.
 */
public class WrongResponseCodeException extends RuntimeException {
  /** Constructs a new runtime exception with the specified detail message.
   * The cause is not initialized, and may subsequently be initialized by a
   * call to {@link #initCause}.
   *
   * @param   message   the detail message. The detail message is saved for
   *          later retrieval by the {@link #getMessage()} method.
   * @param response response of the rest call
   */
  public WrongResponseCodeException(String message, BaseResponse response) {
    super(String.format(
        "Wrong response code caused when %s " +
            " with response code %s with response string:%s",
            message, response.getStatusCode(), response.getContent()));
  }

}
