/**
 * Sends the one-time codes (OTP) by email. Server-only.
 *
 * Modes (chosen by environment variables):
 *  - SMTP_HOST set   -> real email through that SMTP server (for example Gmail or Brevo).
 *  - OTP_CONSOLE=true -> no email, the code is printed in the server log (local development).
 *  - neither         -> email verification is OFF: sign-up works without a code.
 *
 * SMTP_HOST, SMTP_PORT (default 465), SMTP_SECURE (default true for port 465),
 * SMTP_USER, SMTP_PASS, MAIL_FROM.
 */
import nodemailer, { type Transporter } from "nodemailer";

const env = (key: string): string | undefined => {
  const value = process.env[key]?.trim();
  return value ? value : undefined;
};

export const smtpConfigured = Boolean(env("SMTP_HOST"));
const consoleMode = env("OTP_CONSOLE") === "true";

/** True when codes can be delivered (email or console), so verification is required at sign-up. */
export const otpEnabled = smtpConfigured || consoleMode;

let transport: Transporter | undefined;
function getTransport() {
  if (!transport) {
    const port = Number(env("SMTP_PORT") ?? 465);
    const secure =
      (env("SMTP_SECURE") ?? (port === 465 ? "true" : "false")) === "true";
    const user = env("SMTP_USER");
    transport = nodemailer.createTransport({
      host: env("SMTP_HOST"),
      port,
      secure,
      auth: user ? { user, pass: env("SMTP_PASS") ?? "" } : undefined,
      connectionTimeout: 8_000,
      greetingTimeout: 8_000,
      socketTimeout: 12_000,
    });
  }
  return transport;
}

export type OtpType =
  "sign-in" | "email-verification" | "forget-password" | "change-email";

function subjectFor(type: OtpType, otp: string): string {
  if (type === "forget-password")
    return `${otp} is your Meridian password reset code`;
  return `${otp} is your Meridian verification code`;
}

export async function sendOtpEmail({
  to,
  otp,
  type,
}: {
  to: string;
  otp: string;
  type: OtpType;
}) {
  if (!smtpConfigured) {
    // Console mode (development only). Never used when real email is configured.
    console.log(`[auth] ${type} code for ${to}: ${otp}`);
    return;
  }
  const minutes = 5;
  const text = [
    `Your Meridian code is ${otp}`,
    "",
    `It expires in ${minutes} minutes. If you did not ask for it, you can ignore this email.`,
  ].join("\n");
  const html = `<div style="font-family:Arial,sans-serif;max-width:420px;margin:0 auto;padding:24px;color:#111">
  <p style="margin:0 0 8px;font-size:14px;color:#555">Meridian</p>
  <p style="margin:0 0 16px;font-size:16px">Use this code to continue:</p>
  <p style="margin:0 0 16px;font-size:32px;letter-spacing:8px;font-weight:600">${otp}</p>
  <p style="margin:0;font-size:13px;color:#555">It expires in ${minutes} minutes. If you did not ask for it, you can ignore this email.</p>
</div>`;
  await getTransport().sendMail({
    from:
      env("MAIL_FROM") ?? env("SMTP_USER") ?? "Meridian <no-reply@localhost>",
    to,
    subject: subjectFor(type, otp),
    text,
    html,
  });
}
