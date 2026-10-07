export const HOST = process.env.PULSE_E2E_HOST ?? "127.0.0.1";
export const PORT = Number(process.env.PULSE_E2E_PORT ?? "8099");

export interface AppCreds {
  appId: string;
  key: string;
  secret: string;
}

export const APP: AppCreds = {
  appId: "e2e-app",
  key: "e2e-key",
  secret: "e2e-secret",
};

export const APP_NO_CLIENT: AppCreds = {
  appId: "e2e-no-client",
  key: "e2e-no-client-key",
  secret: "e2e-no-client-secret",
};

export const APP_RATELIMITED: AppCreds = {
  appId: "e2e-ratelimited",
  key: "e2e-ratelimited-key",
  secret: "e2e-ratelimited-secret",
};
