import { SignJWT, importPKCS8 } from "@dataland-workerlibs/jose";

// taken from https://github.com/kitsonk/deno-gcp-admin/blob/main/auth.ts

export interface ServiceAccountJSON {
  client_email: string;
  private_key: string;
  private_key_id: string;
}

const ALG = "RS256";

function assert(cond: boolean, message = "Assertion error") {
  if (!cond) {
    throw new Error(message);
  }
}

interface OAuth2TokenJson {
  access_token: string;
  scope?: string;
  token_type: string;
  expires_in: number;
}

/** A class that wraps the response from the Google APIs OAuth2 token service. */
export class OAuth2Token {
  #created = Date.now();
  #json: OAuth2TokenJson;

  /** The raw access token string. */
  get accessToken(): string {
    return this.#json.access_token;
  }
  /** Returns if the `true` if the token has expired, otherwise `false`. */
  get expired(): boolean {
    return this.expiresIn <= 0;
  }
  /** The number of seconds until the token expires. If less than or equal to 0
   * then the token has expired. */
  get expiresIn(): number {
    return this.#created + this.#json.expires_in - Date.now();
  }
  /** Any scopes returned in the authorization response. */
  get scope(): string | undefined {
    return this.#json.scope;
  }
  /** The type of token that was returned. */
  get tokenType(): string {
    return this.#json.token_type;
  }

  constructor(json: OAuth2TokenJson) {
    this.#json = json;
  }

  /** Returns the token as a value for an `Authorization:` header. */
  toString(): string {
    return `${this.#json.token_type} ${this.#json.access_token}`;
  }
}

/** Generates an OAuth2 token against Google APIs for the provided service
 * account and scopes.  Provides an instance of {@linkcode OAuth2Token} that
 * wraps the response from Google API OAuth2 service.
 *
 * ### Example
 *
 * ```ts
 * import { createOAuth2Token } from "https://deno.land/x/deno_gcp_admin/auth.ts";
 * import keys from "./service-account.json" asserts { type: "json" };
 *
 * const token = await createOAuth2Token(
 *   keys,
 *   "https://www.googleapis.com/auth/cloud-platform"
 * );
 *
 * const response = fetch("https://example.googleapis.com/", {
 *   headers: {
 *     authorization: token.toString(),
 *   }
 * });
 * ```
 *
 * @param json A JSON object representing the data from a service account JSON
 *             file obtained from Google Cloud.
 * @param scopes [Scopes](https://developers.google.com/identity/protocols/oauth2/scopes)
 *               that the authorization is being requested for.
 */
export async function createOAuth2Token(
  json: ServiceAccountJSON,
  ...scopes: string[]
): Promise<OAuth2Token> {
  const AUD = "https://oauth2.googleapis.com/token";
  const key = await importPKCS8(json.private_key, ALG);
  const jwt = await new SignJWT({
    scope: scopes.join(" "),
  })
    .setProtectedHeader({ alg: ALG })
    .setIssuer(json.client_email)
    .setSubject(json.client_email)
    .setAudience(AUD)
    .setIssuedAt()
    .setExpirationTime("1h")
    .sign(key);

  const res = await fetch(AUD, {
    method: "POST",
    body: new URLSearchParams([
      ["grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"],
      ["assertion", jwt],
    ]),
    headers: { "content-type": "application/x-www-form-urlencoded" },
  });
  assert(
    res.status === 200,
    `Unexpected authorization response ${res.status} - ${res.statusText}.`
  );
  return new OAuth2Token(await res.json());
}

/** Generates a custom token that can be used with Firebase's
 * `signInWithCustomToken()` API. */
export async function createCustomToken(
  json: ServiceAccountJSON,
  claims?: Record<string, unknown>
): Promise<string> {
  const AUD =
    "https://identitytoolkit.googleapis.com/google.identity.identitytoolkit.v1.IdentityToolkit";
  const key = await importPKCS8(json.private_key, ALG);
  return new SignJWT({
    uid: json.private_key_id,
    claims,
  })
    .setProtectedHeader({ alg: ALG })
    .setIssuer(json.client_email)
    .setSubject(json.client_email)
    .setAudience(AUD)
    .setIssuedAt()
    .setExpirationTime("1h")
    .sign(key);
}
