import z from "zod";

const clientBaseT = z.object({
  FirstName: z.string(),
  LastName: z.string(),
  Email: z.string().nullable(),
  MobilePhone: z.string().nullable(),
  IsProspect: z.boolean(),
  AppointmentGenderPreference: z.string(),
  BirthDate: z.string().nullable(),
  Country: z.string(),
  FirstAppointmentDate: z.string().nullable(),
  LiabilityRelease: z.boolean(),
  Notes: z.string().nullable(),
  SendAccountEmails: z.boolean(),
  SendPromotionalEmails: z.boolean(),
  SendScheduleEmails: z.boolean(),
  State: z.string().nullable(),
  RedAlert: z.string().nullable(),
  YellowAlert: z.string().nullable(),
  MiddleName: z.string().nullable(),
  MobileProvider: z.number().nullable(),
  HomePhone: z.string().nullable(),
  WorkPhone: z.string().nullable(),
  WorkExtension: z.string().nullable(),
  CustomClientFields: z.array(z.unknown()).nullable(),
  AddressLine1: z.string().nullable(),
  AddressLine2: z.string().nullable(),
  City: z.string().nullable(),
  PostalCode: z.string().nullable(),
  ReferredBy: z.string().nullable(),
  EmergencyContactInfoName: z.string().nullable(),
  EmergencyContactInfoEmail: z.string().nullable(),
  EmergencyContactInfoPhone: z.string().nullable(),
  EmergencyContactInfoRelationship: z.string().nullable(),
  Gender: z.string(),
});

const clientPostOnlyT = z.object({
  SuspensionInfo: z.object({
    BookingSuspended: z.boolean(),
  }),
  ProspectStage: z
    .object({
      Id: z.number().nullable(),
    })
    .nullable(),
  HomeLocation: z
    .object({
      Id: z.number(),
    })
    .nullable(),
});

const idT = z.object({ Id: z.string() });
const clientReadOnlyT = z
  .object({
    Id: z.string(),
    ProspectStage: z
      .object({
        Active: z.boolean(),
        Id: z.number(),
        Description: z.string(),
      })
      .nullable(),
    Status: z.string(), // NOTE(gab): seems uneditable
    CreationDate: z.string(),
    ClientCreditCard: z // TODO(gab): figure out if read only. also might need to pass number/type and both exp to go through.
      .object({
        Address: z.string().nullable(),
        CardHolder: z.string().nullable(),
        CardNumber: z.string(),
        CardType: z.string().nullable(),
        City: z.string().nullable(),
        ExpMonth: z.string(),
        ExpYear: z.string(),
        LastFour: z.string(),
        PostalCode: z.string().nullable(),
        State: z.string().nullable(),
      })
      .nullable(),
    ClientIndexes: z.array(z.unknown()),
    ClientRelationships: z.array(z.unknown()),
    IsCompany: z.boolean(),
    Liability: z.object({
      AgreementDate: z.string().nullable(),
      IsReleased: z.boolean(),
      ReleasedBy: z.number().nullable(),
    }),
    MembershipIcon: z.number(), // TODO(gab): might be editable, says not in scope in notion?
    // NOTE(gab): user must opt in to receive mobile text, cannot update from api
    SendAccountTexts: z.boolean(),
    SendPromotionalTexts: z.boolean(),
    SendScheduleTexts: z.boolean(),
    UniqueId: z.number(),
    LastModifiedDateTime: z.string(),
    AccountBalance: z.number(),
    SuspensionInfo: z.object({
      BookingSuspended: z.boolean(),
      SuspensionStartDate: z.string().nullable(),
      SuspensionEndDate: z.string().nullable(),
    }),
    PhotoUrl: z.string().nullable(), // NOTE(gab): ignores writeback
    LastFormulaNotes: z.string().nullable(), // TODO(gab): not demo scope
    Active: z.boolean(), // TODO(gab): not demo scope
    SalesReps: z.array(z.unknown()), // TODO(gab): not demo scope
    Action: z.string(), // TODO(gab): not demo scope
    HomeLocation: z
      .object({
        AdditionalImageURLs: z.array(z.unknown()), // check for data in api
        Address: z.string(),
        Address2: z.string(),
        Amenities: z.string().nullable(),
        BusinessDescription: z.string().nullable(),
        City: z.string(),
        Description: z.string().nullable(),
        HasClasses: z.boolean(),
        Id: z.number(),
        Latitude: z.number(),
        Longitude: z.number(),
        Name: z.string(),
        Phone: z.string(),
        PhoneExtension: z.string(),
        PostalCode: z.string(),
        SiteID: z.number().nullable(),
        StateProvCode: z.string(),
        Tax1: z.number(),
        Tax2: z.number(),
        Tax3: z.number(),
        Tax4: z.number(),
        Tax5: z.number(),
        TotalNumberOfRatings: z.number(),
        AverageRating: z.number(),
        TotalNumberOfDeals: z.number(),
      })
      .nullable(),
    LockerNumber: z.string().nullable(), // TODO(gab): has no specified comments
  })
  .merge(idT);

// NOTE(gab): Contains all properties of the imported client.
// Strict mode will make import abort if unknown properties are sent.
export const clientT = clientBaseT.merge(clientReadOnlyT).strict();

// NOTE(gab):
// 1) Merginging all base columns that are both readable and postable with only postable.
// 2) Make all columns optional by declaring it partial.
// 3) Add the Id columns afterwards, making it the only required column.
// Id is readonly, but is required to identify a client on posting updates.
export const updateClientT = clientBaseT
  .merge(clientPostOnlyT)
  .partial()
  .merge(idT);

export const addClientT = clientBaseT
  .merge(clientPostOnlyT)
  .partial()
  // NOTE(gab): Required fields on adding client.
  .merge(z.object({ FirstName: z.string(), LastName: z.string() }));

export type Client = z.infer<typeof clientT>;
export type UpdateClient = z.infer<typeof updateClientT>;
export type AddClient = z.infer<typeof addClientT>;
